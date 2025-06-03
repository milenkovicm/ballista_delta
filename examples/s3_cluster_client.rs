use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::object_store::session_state_with_s3_support;
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;
//
// docker run -ti -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
//
#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "cwwJtKoaYfl4fR2iW8cL");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "orZWdeTUmmbWWoe1MKSIC3f3aDR2FVwD0lNAZigo");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = session_state_with_s3_support(config)?;
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    deltalake::aws::register_handlers(None);

    ctx.register_parquet("p", "s3://ballista/sample/", Default::default())
        .await?;

    let df = ctx.sql("select * from p").await?;
    df.show().await?;

    let table = deltalake::open_table("s3://ballista/people_countries_delta_dask/")
        .await
        .unwrap();

    ctx.register_table("d", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from d").await?;
    df.show().await?;

    Ok(())
}
