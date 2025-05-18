use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::{
    common::{exec_err, Result},
    execution::object_store::ObjectStoreRegistry,
    prelude::SessionContext,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use deltalake::{
    delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec},
    storage::{file::FileStorageBackend, object_store::local::LocalFileSystem},
    ObjectStore,
};
use std::sync::Arc;
use url::Url;

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
        inputs: &[datafusion::logical_expr::LogicalPlan],
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

#[derive(Debug)]
pub struct CustomObjectStoreRegistry {
    local: Arc<LocalFileSystem>,
}

impl Default for CustomObjectStoreRegistry {
    fn default() -> Self {
        Self {
            local: Arc::new(LocalFileSystem::new()),
        }
    }
}

impl ObjectStoreRegistry for CustomObjectStoreRegistry {
    fn register_store(&self, _url: &Url, _store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>> {
        unimplemented!("register_store not supported")
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let scheme = url.scheme();

        //
        // this is a bit of a hack as url which is received is a bit messed up.
        // the one which comes from client is something like:
        // file---Users-marko-git-ballista_delta-data-people_countries_delta_dask
        //
        let root = url
            .host()
            .map(|x| x.to_string())
            .unwrap_or_default()
            .to_string()
            .replace("file--", "")
            .replace("-", "/");

        match scheme {
            "" | "file" => Ok(self.local.clone()),
            "delta-rs" => {
                let store = FileStorageBackend::try_new(root)?;

                Ok(Arc::new(store))
            }

            _ => exec_err!("get_store - store not supported, url {}", url),
        }
    }
}
