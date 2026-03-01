use clap::Parser;

#[derive(Parser)]
#[command(name = "gauss-server", about = "Gauss streaming data server")]
struct Cli {
    /// Path to TOML configuration file.
    #[arg(long, default_value = "config.toml", env = "GAUSS_CONFIG")]
    config: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    tracing::info!(config = %cli.config, "loading configuration");
    let config = match gauss_engine::config::GaussConfig::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to load config");
            std::process::exit(1);
        }
    };

    tracing::info!(
        topics = config.topics.len(),
        processors = config.processors.len(),
        "bootstrapping engine"
    );
    let mut engine = match gauss_engine::bootstrap::Engine::bootstrap(config).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "failed to bootstrap engine");
            std::process::exit(1);
        }
    };

    tracing::info!("gauss-server started, press Ctrl+C to stop");

    // Listen for SIGHUP (config reload) and SIGINT/SIGTERM (shutdown).
    let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to register SIGHUP handler");
            std::process::exit(1);
        }
    };

    loop {
        tokio::select! {
            _ = sighup.recv() => {
                tracing::info!(config = %cli.config, "SIGHUP received, reloading configuration");
                match engine.reload_from_file(&cli.config).await {
                    Ok(()) => tracing::info!("configuration reloaded successfully"),
                    Err(e) => tracing::error!(error = %e, "configuration reload failed (keeping old config)"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down...");
                break;
            }
        }
    }

    engine.shutdown().await;
}
