use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{custom_session_state, BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use deltalake::DeltaOps;
use std::sync::Arc;
use url::Url;

//
// Write does not NOT work on ballista as insert, update, delete
// register their own planners, which will override ballista
// planner, required to run logical plan on ballista cluster
//

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
    let ctx = SessionContext::standalone_with_state(state).await?;

    let url = Url::parse("./data/people_countries_delta_dask").unwrap();
    let table = deltalake::open_table(url).await.unwrap();

    ctx.register_table("demo", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from demo").await?;
    let logical_plan = df.logical_plan().clone();

    let url = Url::parse(&format!(
        "file:{}/data/people_countries_delta_dask",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let delta_ops = DeltaOps::try_from_uri(url).await.unwrap();

    let write = delta_ops
        .write(vec![])
        .with_input_session_state(ctx.state())
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .with_input_execution_plan(logical_plan.into());

    //
    // write will not run on ballista as delta registers its own query planner
    // which will remove ballista query planner (required to run plans on ballista)
    //
    // see:
    // https://github.com/delta-io/delta-rs/blob/2ffaacc9e1d892aa2a9a0d773efb416d9adb8bf5/crates/core/src/operations/write/mod.rs#L426
    //
    // write will be successful (if we ignore partitioning is off), but it will be run on local
    // datafusion context.
    //

    write.await.unwrap();

    Ok(())
}
