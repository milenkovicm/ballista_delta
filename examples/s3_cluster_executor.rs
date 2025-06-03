use ballista_delta::object_store::{runtime_env_with_s3_support, session_config_with_s3_support};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "cwwJtKoaYfl4fR2iW8cL");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "orZWdeTUmmbWWoe1MKSIC3f3aDR2FVwD0lNAZigo");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        override_logical_codec: Some(Arc::new(BallistaDeltaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(BallistaDeltaPhysicalCodec::default())),

        // overriding default config producer with custom producer
        // which has required S3 configuration options
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        // overriding default runtime producer with custom producer
        // which knows how to create S3 connections
        override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),

        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
