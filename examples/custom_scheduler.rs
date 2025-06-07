use ballista_core::error::BallistaError;
use ballista_delta::{custom_session_config, custom_session_state};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use std::net::AddrParseError;
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

    //
    // this is important to be done on scheduler side as well
    // otherwise:
    //
    // `Could not parse logical plan protobuf: Internal error: Error encoding delta table`
    //
    // will be raised
    deltalake::aws::register_handlers(None);

    let config: SchedulerConfig = SchedulerConfig {
        override_logical_codec: Some(Arc::new(BallistaDeltaLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(BallistaDeltaPhysicalCodec::default())),
        override_config_producer: Some(Arc::new(custom_session_config)),
        override_session_builder: Some(Arc::new(custom_session_state)),
        ..Default::default()
    };

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
