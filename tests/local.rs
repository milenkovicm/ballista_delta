use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{custom_session_state, BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    assert_batches_eq,
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;

mod common;

#[tokio::test]
async fn standalone() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;
    let ctx = SessionContext::standalone_with_state(state).await?;

    let table = deltalake::open_table("./data/people_countries_delta_dask")
        .await
        .unwrap();

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

    ctx.register_table("c0", Arc::new(table)).unwrap();

    let result = ctx.sql("select * from c0 order by first_name").await?.collect().await?;
    assert_batches_eq!(expected, &result);

    ctx.sql("create external table c1 stored as delta location './data/people_countries_delta_dask/' ")
        .await?
        .show()
        .await?;

    let result = ctx.sql("select * from c1 order by first_name").await?.collect().await?;
    assert_batches_eq!(expected, &result);

    Ok(())
}
