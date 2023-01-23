use anyhow::Result;
use aws_config::SdkConfig;
use aws_sdk_s3::{
    error::{HeadBucketError, HeadBucketErrorKind},
    model::{Tag, Tagging},
    Client,
};

// TODO: consider if we'd need a database specific s3 provisioner

#[derive(Debug)]
pub struct S3Provisioner {
    s3_client: Client,
}

// FIXME: attach name to span once we have a client for s3 ops
impl S3Provisioner {
    pub fn new(aws_conf: &SdkConfig) -> Self {
        S3Provisioner {
            s3_client: Client::new(aws_conf),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn bucket_exists(&self, name: &String) -> Result<bool> {
        let head_resp = self
            .s3_client
            .head_bucket()
            .bucket(name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        match head_resp {
            Ok(_) => Ok(true),
            Err(HeadBucketError {
                kind: HeadBucketErrorKind::NotFound(_),
                ..
            }) => Ok(false),
            Err(t) => Err(t.into()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_bucket(&self, name: &String) -> Result<()> {
        // FIXME: location contraint not being set means this needs to be in use1
        // TODO: consider handling BucketAlreadyOwnedByYou
        self.s3_client
            .create_bucket()
            .bucket(name)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        // NOTE: this will overwrite existing tags, its fine since we just created the bucket, and don't care about
        //       anyone racing us (we should own the resource).
        self.s3_client
            .put_bucket_tagging()
            .bucket(name)
            .tagging(
                Tagging::builder()
                    // TODO: read all of this from config
                    .tag_set(Tag::builder().key("provisioner").value("basin").build())
                    .tag_set(Tag::builder().key("subprovisioner").value("s3").build())
                    .tag_set(Tag::builder().key("version").value("0.0.1").build())
                    .build(),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }
}
