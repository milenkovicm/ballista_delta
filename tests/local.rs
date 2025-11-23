use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{custom_session_state, BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    assert_batches_eq,
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;

mod common;

#[tokio::test]
async fn standalone() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;
    let ctx = SessionContext::standalone_with_state(state).await?;

    let url = Url::parse(&format!(
        "file:{}/data/people_countries_delta_dask",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let table = deltalake::open_table(url).await.unwrap();

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

#[tokio::test]
async fn standalone_insert() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;
    let ctx = SessionContext::standalone_with_state(state).await?;

    ctx.sql("create external table c0 stored as delta location './data/people_countries_delta_dask/' ")
        .await?
        .show()
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

    let result = ctx.sql("select * from c0 order by first_name").await?.collect().await?;
    assert_batches_eq!(expected, &result);

    create_target_copy();

    ctx.sql("create external table c1 stored as delta location './target/data/people_countries_delta_dask/' ")
        .await?
        .show()
        .await?;

    let result = ctx.sql("INSERT into c1 select * from c0").await?.collect().await;
    // DataFusionError(NotImplemented(\"Insert into not implemented for this table\"))"
    assert!(result.is_err());

    Ok(())
}

// a bit optimistic take to copy but will work for now
fn create_target_copy() {
    {
        let src = format!("./data/people_countries_delta_dask");
        let dst = format!("target/data/people_countries_delta_dask");

        // remove destination if it exists
        let _ = fs::remove_dir_all(&dst);

        fn copy_dir_recursive(src: &Path, dst: &Path) {
            fs::create_dir_all(dst).unwrap();
            for entry in fs::read_dir(src).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                let dest = dst.join(entry.file_name());
                if path.is_dir() {
                    copy_dir_recursive(&path, &dest);
                } else {
                    fs::copy(&path, &dest).unwrap();
                }
            }
        }

        copy_dir_recursive(Path::new(&src), Path::new(&dst));
    }
}
