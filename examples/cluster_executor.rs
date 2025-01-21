use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec, CustomObjectStoreRegistry};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use datafusion::{
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    prelude::SessionConfig,
};
use std::sync::Arc;
///
/// # Custom Ballista Executor
///
/// This example demonstrates how to crate custom ballista executors with support
/// for custom logical and physical codecs.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        override_logical_codec: Some(Arc::new(BallistaDeltaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(BallistaDeltaPhysicalCodec::default())),

        override_runtime_producer: Some(Arc::new(|_: &SessionConfig| {
            let runtime_config =
                RuntimeConfig::new().with_object_store_registry(Arc::new(CustomObjectStoreRegistry::default()));
            let runtime_env = RuntimeEnv::try_new(runtime_config)?;

            Ok(Arc::new(runtime_env))
        })),

        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
