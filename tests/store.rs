use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::custom_session_state;
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::assert_batches_eq;
use datafusion::{
    common::Result,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use url::Url;

mod common;

//
// docker run -ti --rm -e MINIO_ACCESS_KEY=MINIO -e MINIO_SECRET_KEY=MINIOSECRET -p 9000:9000 -p 9001:9001 minio/minio:RELEASE.2025-05-24T17-08-30Z server /data --console-address ":9001"
//
#[tokio::test]
async fn standalone() -> Result<()> {
    let container = crate::common::create_minio_container();
    let node = container.start().await.unwrap();

    let port = node.get_host_port_ipv4(9000).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{}", port);

    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", common::ACCESS_KEY_ID);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", common::SECRET_KEY);
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    common::upload_test_directory_to_s3_bucket(&endpoint, "people_countries_delta_dask", "ballista")
        .await
        .expect("dir to be uploaded");

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(BallistaDeltaLogicalCodec::default()))
        .with_ballista_physical_extension_codec(Arc::new(BallistaDeltaPhysicalCodec::default()));

    let state = custom_session_state(config)?;
    let ctx = SessionContext::standalone_with_state(state).await?;

    deltalake::aws::register_handlers(None);

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
    let url = Url::parse("s3://ballista/people_countries_delta_dask/").expect("valid path");
    let table = deltalake::open_table(url).await.unwrap();

    ctx.register_table("c0", Arc::new(table)).unwrap();

    let result = ctx.sql("select * from c0 order by first_name").await?.collect().await?;
    assert_batches_eq!(expected, &result);

    ctx.sql("create external table c1 stored as delta location 's3://ballista/people_countries_delta_dask/' ")
        .await?
        .show()
        .await?;

    let result = ctx.sql("select * from c1 order by first_name").await?.collect().await?;

    assert_batches_eq!(expected, &result);

    Ok(())
}
