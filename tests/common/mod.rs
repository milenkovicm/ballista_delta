use std::error::Error;

use aws_config::Region;
use aws_sdk_s3::Client;
use testcontainers_modules::{
    minio::{self, MinIO},
    testcontainers::{ContainerRequest, ImageExt},
};
use walkdir::WalkDir;

pub const ACCESS_KEY_ID: &str = "MINIO";
pub const SECRET_KEY: &str = "MINIOSECRET";
pub const TEST_DATA: &str = "./data";

#[ctor::ctor]
fn init() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();
}

#[allow(dead_code)]
pub fn create_minio_container() -> ContainerRequest<minio::MinIO> {
    MinIO::default()
        .with_env_var("MINIO_ACCESS_KEY", ACCESS_KEY_ID)
        .with_env_var("MINIO_SECRET_KEY", SECRET_KEY)
        .with_tag("RELEASE.2025-05-24T17-08-30Z")
}

#[allow(dead_code)]
pub async fn client(endpoint_uri: &str) -> Client {
    let credentials = aws_sdk_s3::config::Credentials::new(ACCESS_KEY_ID, SECRET_KEY, None, None, "test");
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new("local"))
        .endpoint_url(endpoint_uri)
        .credentials_provider(credentials)
        .load()
        .await;

    Client::new(&shared_config)
}

// a rather simple way to upload directory to a bucket
#[allow(dead_code)]
pub async fn upload_test_directory_to_s3_bucket(
    endpoint: &str,
    directory: &str,
    bucket: &str,
) -> Result<(), Box<dyn Error>> {
    let client = client(endpoint).await;
    let _ = client.create_bucket().bucket(bucket).send().await;

    for entry in WalkDir::new(format!("{}/{}", TEST_DATA, directory)) {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let key = path.strip_prefix(TEST_DATA)?.to_string_lossy().to_string();
            let body = tokio::fs::read(&path).await?;
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(body.into())
                .send()
                .await
                .unwrap();
        }
    }

    Ok(())
}
