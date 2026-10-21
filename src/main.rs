//! Command-line entry point.

use std::process::ExitCode;

use clap::Parser;
use ha_ch_ingestor::config::{Config, LogFormat, Overrides};
use ha_ch_ingestor::error::Error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;

const EXIT_CONFIG: u8 = 78;

#[derive(Debug, Parser)]
#[command(
    name = "ha-ch-ingestor",
    version,
    about = "Stream Home Assistant state changes into ClickHouse"
)]
struct Cli {
    /// Print incoming rows as NDJSON on stdout instead of inserting them.
    #[arg(long)]
    dry_run: bool,

    /// Print the resolved configuration (secrets redacted) and exit.
    #[arg(long)]
    print_config: bool,

    /// Log format; overrides LOG_FORMAT.
    #[arg(long, value_enum)]
    log_format: Option<LogFormat>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let overrides = Overrides {
        dry_run: cli.dry_run,
        log_format: cli.log_format,
    };

    let config = match Config::load(overrides) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };

    init_tracing(config.log_format);

    if cli.print_config {
        match serde_json::to_string_pretty(&config.redacted_json()) {
            Ok(rendered) => println!("{rendered}"),
            Err(error) => {
                eprintln!("error: {error}");
                return ExitCode::FAILURE;
            }
        }
        return ExitCode::SUCCESS;
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        dry_run = config.dry_run,
        "starting ha-ch-ingestor"
    );

    match ha_ch_ingestor::run(config, shutdown_signal()).await {
        Ok(stats) => {
            tracing::info!(
                inserted = stats.inserted,
                batches = stats.batches,
                reconnects = stats.reconnects,
                "stopped"
            );
            ExitCode::SUCCESS
        }
        Err(Error::Shutdown) => ExitCode::SUCCESS,
        Err(Error::Config(message)) => {
            eprintln!("config error: {message}");
            ExitCode::from(EXIT_CONFIG)
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        // Logs go to stderr so `--dry-run` can own stdout.
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(error = %error, "could not install the SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("shutdown signal received");
}
