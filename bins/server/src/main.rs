mod config;
mod error;

use clap::Parser;
use config::{Cli, Commands};

mod cmd;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Serve(args) => cmd::serve::run(args).await,
    };
    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
