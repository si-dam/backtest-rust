use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::{LogFormat, Settings};

pub fn init_tracing(settings: &Settings) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(settings.rust_log.clone()));

    match settings.log_format {
        LogFormat::Plain => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().with_target(true))
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_target(true)
                        .with_current_span(true)
                        .with_span_list(true),
                )
                .init();
        }
    }
}
