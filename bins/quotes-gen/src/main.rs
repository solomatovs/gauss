mod cmd;

use clap::Parser;
use cmd::config::{Effective, GenArgs};

#[derive(Parser)]
#[command(name = "quotes-gen", about = "Генератор котировок (plugin-based)")]
struct Cli {
    #[command(flatten)]
    args: GenArgs,
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

    let eff = match Effective::new(&cli.args) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = cmd::generate::run(&eff).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
