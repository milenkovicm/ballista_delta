# Datafusion Ballista Support For Delta Table

Since version `43.0.0` [datafusion ballista](https://github.com/apache/datafusion-ballista) extending core components and functionalities.

This example demonstrate extending [datafusion ballista](https://github.com/apache/datafusion-ballista) to support [delta.rs](https://delta-io.github.io/delta-rs/) read operations.

>
> [!NOTE]
>
> This project has been part of "Extending DataFusion Ballista" show case series
>
> - [DataFusion Ballista Python UDF Support](https://github.com/milenkovicm/ballista_python)
> - [DataFusion Ballista Read Support For Delta Table](https://github.com/milenkovicm/ballista_delta)
> - [Extending DataFusion Ballista](https://github.com/milenkovicm/ballista_extensions)
>

>
> [!IMPORTANT]
>
> This is just a showcase project, it is not meant to be maintained.
>

Setting up [standalone ballista](examples/standalone.rs):

```rust
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_delta::{BallistaDeltaLogicalCodec, BallistaDeltaPhysicalCodec};
use datafusion::{
    common::Result,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
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

    ctx.sql("select * from demo").await?.show().await?;


    Ok(())
}
```

Other examples show extending [client](examples/cluster_client.rs), [scheduler](examples/cluster_scheduler.rs)
and [executor](examples/cluster_executor.rs) for cluster deployment.
