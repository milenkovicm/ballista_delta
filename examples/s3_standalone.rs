use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::custom_session_state;
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;
use url::Url;
//
// docker run -ti --rm -e MINIO_ACCESS_KEY=MINIO -e MINIO_SECRET_KEY=MINIOSECRET -p 9000:9000 -p 9001:9001 minio/minio:RELEASE.2025-05-24T17-08-30Z server /data --console-address ":9001"
//
#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "MINIO");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "MINIOSECRET");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;
    let ctx = SessionContext::standalone_with_state(state).await?;

    deltalake::aws::register_handlers(None);

    ctx.register_parquet("p", "s3://ballista/sample/", Default::default())
        .await?;

    let df = ctx.sql("select * from p").await?;
    df.show().await?;

    let url = Url::parse("s3://ballista/people_countries_delta_dask/").expect("valid path");
    let table = deltalake::open_table(url).await.unwrap();

    ctx.register_table("d", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from d").await?;
    df.show().await?;

    ctx.sql("create external table c stored as delta location 's3://ballista/people_countries_delta_dask/' ")
        .await?
        .show()
        .await?;

    let df = ctx.sql("select * from c").await?;
    df.show().await?;

    Ok(())
}
