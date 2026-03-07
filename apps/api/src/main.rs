use anyhow::Result;
use app_core::{config::Settings, telemetry::init_tracing};
use runtime_api::router;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);

    let app = router::build_router(settings.clone()).await?;
    let listener = TcpListener::bind(settings.api_socket_addr()?).await?;
    tracing::info!(address = %settings.api_socket_addr()?, "runtime-api listening");
    axum::serve(listener, app).await?;

    Ok(())
}
