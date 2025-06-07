use ballista_delta::{custom_runtime_env, custom_session_config};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "MINIO");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "MINIOSECRET");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        override_logical_codec: Some(Arc::new(BallistaDeltaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(BallistaDeltaPhysicalCodec::default())),
        override_config_producer: Some(Arc::new(custom_session_config)),
        override_runtime_producer: Some(Arc::new(custom_runtime_env)),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
