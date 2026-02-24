use std::io::Write;

use super::config::Effective;
use super::error::QuotesGenError;
use super::domain::{new_symbols, FileReader, Rng, Symbol, parse_time};
use super::sink_pipeline::{quote_to_record, Quote, SinkPipeline, load_pipelines};

// ═══════════════════════════════════════════════════════════════
//  Main dispatch
// ═══════════════════════════════════════════════════════════════

pub async fn run(args: &Effective) -> Result<(), QuotesGenError> {
    let mut rng = Rng::new(args.seed);
    let mut symbols = new_symbols();

    if let Some(ref sym) = args.symbol {
        symbols.retain(|s| s.name.eq_ignore_ascii_case(sym));
        if symbols.is_empty() {
            let names: Vec<_> = new_symbols().iter().map(|s| s.name).collect();
            return Err(QuotesGenError::Config(format!(
                "unknown symbol: {sym}\navailable: {}",
                names.join(" ")
            )));
        }
    }

    if args.price > 0.0 && symbols.len() == 1 {
        symbols[0].price = args.price;
    }

    // Load sink pipelines
    let mut pipelines = load_pipelines(&args.sinks)?;

    // Start transports and connect
    for p in &mut pipelines {
        p.endpoint.transport.start()?;
        p.ensure_connected()?;
        tracing::info!(sink = %p.name(), "connected");
    }

    if args.generate {
        let from = args
            .from
            .as_deref()
            .ok_or_else(|| QuotesGenError::Config("--from is required with --generate".into()))?;
        let to = args
            .to
            .as_deref()
            .ok_or_else(|| QuotesGenError::Config("--to is required with --generate".into()))?;
        let from_sec = parse_time(from)?;
        let to_sec = parse_time(to)?;
        if to_sec <= from_sec {
            return Err(QuotesGenError::Config("--to must be after --from".into()));
        }
        run_generate(args, &mut pipelines, &mut symbols, &mut rng, from_sec, to_sec).await
    } else if let Some(ref path) = args.file {
        let path = path.clone();
        run_file(args, &mut pipelines, &path).await
    } else {
        run_stream(args, &mut pipelines, &mut symbols, &mut rng).await
    }
}

// ═══════════════════════════════════════════════════════════════
//  Generate mode: stdout + sinks
// ═══════════════════════════════════════════════════════════════

async fn run_generate(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
    from_sec: i64,
    to_sec: i64,
) -> Result<(), QuotesGenError> {
    let interval_ms = args.interval as i64;
    let from_ms = from_sec * 1000;
    let to_ms = to_sec * 1000;

    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let mut quotes: Vec<Quote> = Vec::new();

    let mut t_ms = from_ms;
    while t_ms < to_ms {
        for sym in symbols.iter_mut() {
            sym.tick(rng);
            let jitter = rng.next_intn(interval_ms as usize) as i64;
            let ts = t_ms + jitter;
            let quote = sym.to_quote_ts(ts);
            // Serialize for stdout using first pipeline's codec
            if let Some(p) = pipelines.first() {
                let record = quote_to_record(&quote)?;
                if let Ok(data) = p.endpoint.codec.encode(&record.value) {
                    let _ = out.write_all(&data);
                    let _ = out.write_all(b"\n");
                }
            }
            quotes.push(quote);
        }
        t_ms += interval_ms;
    }
    out.flush().ok();

    let duration = to_sec - from_sec;
    tracing::info!(
        ticks = quotes.len(),
        symbols = symbols.len(),
        duration_s = duration,
        interval_ms,
        "generated ticks"
    );

    // Send to sinks
    tracing::info!(count = quotes.len(), "sending to sinks");
    let start = std::time::Instant::now();

    if args.rate <= 0.0 {
        // Batch send
        for (i, quote) in quotes.iter().enumerate() {
            for p in pipelines.iter_mut() {
                p.send_quote(quote)?;
            }
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                eprint!(
                    "\r  {}/{} sent ({:.1} msg/s)",
                    i + 1,
                    quotes.len(),
                    (i + 1) as f64 / elapsed.as_secs_f64()
                );
            }
        }
    } else {
        // Rate-limited send
        let interval = std::time::Duration::from_secs_f64(1.0 / args.rate);
        for (i, quote) in quotes.iter().enumerate() {
            for p in pipelines.iter_mut() {
                p.send_quote(quote)?;
            }
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                eprint!(
                    "\r  {}/{} sent ({:.1} msg/s)",
                    i + 1,
                    quotes.len(),
                    (i + 1) as f64 / elapsed.as_secs_f64()
                );
            }
            if i < quotes.len() - 1 {
                tokio::time::sleep(interval).await;
            }
        }
    }

    let elapsed = start.elapsed();
    tracing::info!(
        sent = quotes.len(),
        elapsed_s = format_args!("{:.1}", elapsed.as_secs_f64()),
        rate = format_args!("{:.1}", quotes.len() as f64 / elapsed.as_secs_f64()),
        "send complete"
    );

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  Stream mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_stream(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    println!("Quote Generator (plugin-based)");
    println!("  sinks   : {}", pipelines.iter().map(|p| p.name()).collect::<Vec<_>>().join(", "));
    if let Some(ref sym) = args.symbol {
        println!("  symbol  : {sym}");
    }
    if args.history {
        println!("  ts_ms   : yes");
    }

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_random_auto(args, pipelines, symbols, rng).await
    } else {
        println!();
        println!("Commands: Enter -- send 1 random quote | N -- send N quotes | q -- quit");
        println!();
        run_random_interactive(args, pipelines, symbols, rng).await
    }
}

async fn run_random_interactive(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    let stdin = std::io::stdin();

    loop {
        print!("> ");
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            break;
        }

        if input.is_empty() {
            let idx = rng.next_intn(symbols.len());
            symbols[idx].tick(rng);
            let quote = symbols[idx].to_quote(args.history);
            for p in pipelines.iter_mut() {
                p.send_quote_reconnect(&quote)?;
            }
            // Print serialized form
            if let Some(p) = pipelines.first() {
                let record = quote_to_record(&quote)?;
                if let Ok(data) = p.endpoint.codec.encode(&record.value) {
                    println!("  -> {}", String::from_utf8_lossy(&data));
                }
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    for _ in 0..n {
                        let idx = rng.next_intn(symbols.len());
                        symbols[idx].tick(rng);
                        let quote = symbols[idx].to_quote(args.history);
                        if let Some(p) = pipelines.first() {
                            let record = quote_to_record(&quote)?;
                            if let Ok(data) = p.endpoint.codec.encode(&record.value) {
                                println!("  -> {}", String::from_utf8_lossy(&data));
                            }
                        }
                        for p in pipelines.iter_mut() {
                            p.send_quote_reconnect(&quote)?;
                        }
                    }
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    Ok(())
}

async fn run_random_auto(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / args.rate));
    let mut sent = 0u64;
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                break;
            }
            _ = interval.tick() => {
                let idx = rng.next_intn(symbols.len());
                symbols[idx].tick(rng);
                let quote = symbols[idx].to_quote(args.history);
                for p in pipelines.iter_mut() {
                    if let Err(e) = p.send_quote_reconnect(&quote) {
                        tracing::error!(sink = %p.name(), error = ?e, "send error");
                    }
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  File mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_file(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    path: &str,
) -> Result<(), QuotesGenError> {
    let mut fr = FileReader::open(path)?;

    println!("Quote Generator (plugin-based)");
    println!("  sinks   : {}", pipelines.iter().map(|p| p.name()).collect::<Vec<_>>().join(", "));
    println!("  mode    : file ({}, {} lines)", fr.path, fr.total());

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_file_auto(args, pipelines, &mut fr).await
    } else {
        println!();
        println!("Commands: Enter -- send next | N -- send N lines | q -- quit");
        println!();
        run_file_interactive(pipelines, &mut fr).await
    }
}

async fn run_file_interactive(
    pipelines: &mut [SinkPipeline],
    fr: &mut FileReader,
) -> Result<(), QuotesGenError> {
    let stdin = std::io::stdin();

    while !fr.done() {
        print!("[{}/{}] > ", fr.total() - fr.remaining(), fr.total());
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            return Ok(());
        }

        if input.is_empty() {
            if let Some(line) = fr.next_line() {
                let line = line.to_string();
                println!("  -> {line}");
                for p in pipelines.iter_mut() {
                    p.send_raw_reconnect(line.as_bytes())?;
                }
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    let lines: Vec<String> = fr.next_n(n).to_vec();
                    if lines.is_empty() {
                        println!("  EOF");
                        continue;
                    }
                    for l in &lines {
                        println!("  -> {l}");
                        for p in pipelines.iter_mut() {
                            p.send_raw_reconnect(l.as_bytes())?;
                        }
                    }
                    println!("  sent {} lines", lines.len());
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    println!("  EOF -- all lines sent");
    Ok(())
}

async fn run_file_auto(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    fr: &mut FileReader,
) -> Result<(), QuotesGenError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / args.rate));
    let mut sent = 0u64;
    let total = fr.total();
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    while !fr.done() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                return Ok(());
            }
            _ = interval.tick() => {
                let line = match fr.next_line() {
                    Some(l) => l.to_string(),
                    None => break,
                };
                for p in pipelines.iter_mut() {
                    if let Err(e) = p.send_raw_reconnect(line.as_bytes()) {
                        tracing::error!(sink = %p.name(), error = ?e, "send error");
                    }
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent}/{total} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\r  done: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
