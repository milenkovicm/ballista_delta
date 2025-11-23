use ballista::prelude::SessionConfigExt;
use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::catalog::TableProviderFactory;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionConfig;
use datafusion::{common::Result, prelude::SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use deltalake::delta_datafusion::DeltaTableFactory;
use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec, DeltaScan};
use std::sync::Arc;

use crate::object_store::CustomObjectStoreRegistry;

pub mod object_store;

pub fn custom_session_config() -> SessionConfig {
    SessionConfig::new_with_ballista().with_information_schema(true)
}

pub fn custom_runtime_env(_session_config: &SessionConfig) -> Result<Arc<RuntimeEnv>> {
    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(CustomObjectStoreRegistry::default()))
        .build()?;

    Ok(Arc::new(runtime_env))
}

pub fn custom_session_state(session_config: SessionConfig) -> datafusion::common::Result<SessionState> {
    let runtime_env = custom_runtime_env(&session_config)?;

    Ok(SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .with_config(session_config)
        .with_default_features()
        .with_table_factory("DELTA".to_string(), Arc::new(DeltaTableFactory {}))
        // this is experimental table provider to support
        // insert into
        .with_table_factory("DELTA_INSERT".to_string(), Arc::new(CustomDeltaTableFactory {}))
        .build())
}

pub fn custom_state() -> datafusion::common::Result<SessionState> {
    custom_session_state(custom_session_config())
}

#[derive(Debug)]
pub struct BallistaDeltaLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
    delta: DeltaLogicalCodec,
}

impl Default for BallistaDeltaLogicalCodec {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            delta: DeltaLogicalCodec {},
        }
    }
}

impl datafusion_proto::logical_plan::LogicalExtensionCodec for BallistaDeltaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &datafusion::logical_expr::Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: deltalake::arrow::datatypes::SchemaRef,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        self.delta.try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.delta.try_encode_table_provider(table_ref, node, buf)
    }
}

#[derive(Debug)]
pub struct BallistaDeltaPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
    delta: DeltaPhysicalCodec,
}

impl Default for BallistaDeltaPhysicalCodec {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            delta: DeltaPhysicalCodec {},
        }
    }
}

impl PhysicalExtensionCodec for BallistaDeltaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        // NOTE: check note below
        if let Ok(r) = self.delta.try_decode(buf, inputs, registry) {
            r.as_any().downcast_ref::<DeltaScan>();
            Ok(r)
        } else {
            self.inner.try_decode(buf, inputs, registry)
        }
    }

    fn try_encode(&self, node: Arc<dyn datafusion::physical_plan::ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        // NOTE: this is not really correct
        //       we need to capture which encoder actually encoded
        //       node. using proto oneof would be appropriate
        if let Ok(r) = self.delta.try_encode(node.clone(), buf) {
            Ok(r)
        } else {
            self.inner.try_encode(node, buf)
        }
    }
}

// custom table factory which uses DeltaTableProvider
// instead of default one
//
// NOTE: it does not work as it is not serializable
#[derive(Debug, Default)]
pub struct CustomDeltaTableFactory {}

#[async_trait::async_trait]
impl TableProviderFactory for CustomDeltaTableFactory {
    async fn create(
        &self,
        _state: &dyn datafusion::catalog::Session,
        cmd: &datafusion::logical_expr::CreateExternalTable,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let table = if cmd.options.is_empty() {
            let table_url = deltalake::ensure_table_uri(&cmd.to_owned().location)?;
            deltalake::open_table(table_url).await?
        } else {
            let table_url = deltalake::ensure_table_uri(&cmd.to_owned().location)?;
            deltalake::open_table_with_storage_options(table_url, cmd.to_owned().options).await?
        };

        let scan_config = deltalake::delta_datafusion::DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let table_provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
            table.snapshot().unwrap().snapshot().clone(),
            table.log_store(),
            scan_config,
        )
        .unwrap();

        Ok(Arc::new(table_provider))
    }
}
