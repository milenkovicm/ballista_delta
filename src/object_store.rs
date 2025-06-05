use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::{ClientOptions, ObjectStore};
use std::sync::Arc;
use url::Url;

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
        None
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let scheme = url.scheme();

        match scheme {
            "" | "file" => Ok(self.local.clone()),
            "http" | "https" => Ok(Arc::new(
                HttpBuilder::new()
                    .with_client_options(ClientOptions::new().with_allow_http(true))
                    .with_url(url.origin().ascii_serialization())
                    .build()?,
            )),
            "s3" => {
                let bucket_name = Self::get_bucket_name(url)?;
                let s3store = AmazonS3Builder::from_env().with_bucket_name(bucket_name).build()?;

                Ok(Arc::new(s3store))
            }
            "delta-rs"
                if url
                    .host()
                    .map(|s| s.to_string().to_ascii_lowercase().starts_with("s3-"))
                    .unwrap_or(false) =>
            {
                // we should make this more robust
                let delta_url = url
                    .host()
                    .map(|x| x.to_string())
                    .unwrap_or_default()
                    .to_string()
                    .replace("s3-", "s3://")
                    .replace("-", "/");
                let url = Url::parse(&delta_url)
                    .map_err(|_| DataFusionError::Execution("can't parse `delta-rs` url".to_string()))?;

                let s3store = AmazonS3Builder::from_env()
                    .with_bucket_name(Self::get_bucket_name(&url)?)
                    .build()?;

                Ok(Arc::new(PrefixStore::new(s3store, url.path())))
            }

            "delta-rs"
                if url
                    .host()
                    .map(|s| s.to_string().to_ascii_lowercase().starts_with("file-"))
                    .unwrap_or(false) =>
            {
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
                let store = deltalake::storage::file::FileStorageBackend::try_new(root)?;

                Ok(Arc::new(store))
            }

            _ => exec_err!("get_store - store not supported, url {}", url),
        }
    }
}

impl CustomObjectStoreRegistry {
    fn get_bucket_name(url: &Url) -> Result<&str> {
        url.host_str().ok_or_else(|| {
            DataFusionError::Execution(format!("Not able to parse bucket name from url: {}", url.as_str()))
        })
    }
}
