use ballista_core::error::BallistaError;
use ballista_delta::object_store::{session_config_with_s3_support, session_state_with_s3_support};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use std::net::AddrParseError;
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
        // overriding default runtime producer with custom producer
        // which knows how to create S3 connections
        override_config_producer: Some(Arc::new(session_config_with_s3_support)),
        // overriding default session builder, which has custom session configuration
        // runtime environment and session state.
        override_session_builder: Some(Arc::new(session_state_with_s3_support)),
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
