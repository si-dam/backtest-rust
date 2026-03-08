use std::path::Path;

use anyhow::{Context, Result};
use tokio::fs;

use crate::config::Settings;

pub async fn prepare_runtime_directories(
    settings: &Settings,
    include_ingest_root: bool,
) -> Result<()> {
    create_dir_if_missing(&settings.artifact_root)
        .await
        .with_context(|| format!("failed to prepare artifact root {}", settings.artifact_root.display()))?;

    if include_ingest_root {
        create_dir_if_missing(&settings.ingest_root)
            .await
            .with_context(|| format!("failed to prepare ingest root {}", settings.ingest_root.display()))?;
    }

    Ok(())
}

async fn create_dir_if_missing(path: &Path) -> Result<()> {
    fs::create_dir_all(path).await?;
    Ok(())
}

pub async fn shutdown_signal(component: &'static str) {
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};

        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                tracing::warn!(component, error = %error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::warn!(component, error = %error, "failed to install CTRL+C handler");
            }
        }
        _ = terminate => {}
    }

    tracing::info!(component, "shutdown signal received");
}
