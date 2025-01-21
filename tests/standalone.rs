use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    assert_batches_eq,
    common::Result,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;

#[tokio::test]
async fn standalone() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let table = deltalake::open_table("./data/people_countries_delta_dask")
        .await
        .unwrap();

    let ctx = SessionContext::standalone_with_state(state).await?;

    ctx.register_table("demo", Arc::new(table)).unwrap();

    let result = ctx
        .sql("select * from demo order by first_name")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+------------+-----------+-----------+-----------+",
        "| first_name | last_name | continent | country   |",
        "+------------+-----------+-----------+-----------+",
        "| Bruce      | Lee       | Asia      | China     |",
        "| Ernesto    | Guevara   | NaN       | Argentina |",
        "| Jack       | Ma        | Asia      | China     |",
        "| Soraya     | Jala      | NaN       | Germany   |",
        "| Wolfgang   | Manche    | NaN       | Germany   |",
        "+------------+-----------+-----------+-----------+",
    ];

    assert_batches_eq!(expected, &result);

    Ok(())
}
