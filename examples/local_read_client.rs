use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{custom_session_state, BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;

    let url = Url::parse(&format!(
        "file:{}/data/people_countries_delta_dask",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let table = deltalake::open_table(url).await.unwrap();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    ctx.register_table("demo", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from demo").await?;

    df.show().await?;

    Ok(())
}
