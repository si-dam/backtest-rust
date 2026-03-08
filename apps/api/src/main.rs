use anyhow::Result;
use app_core::{
    config::Settings,
    runtime::{prepare_runtime_directories, shutdown_signal},
    telemetry::init_tracing,
};
use runtime_api::router;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);
    prepare_runtime_directories(&settings, true).await?;

    let app = router::build_router(settings.clone()).await?;
    let listener = TcpListener::bind(settings.api_socket_addr()?).await?;
    tracing::info!(address = %settings.api_socket_addr()?, "runtime-api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal("runtime-api"))
        .await?;

    Ok(())
}
