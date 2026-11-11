//! Credentials for a table's files on GCS (REST-11), against a store that holds a downscoped token to its boundary: the
//! server's service account is given a token at its `token_uri`, which Google's STS downscopes to a table's prefix; it
//! reads that table's objects and not another table's, and writes them only when minted for a writer. Needs an emulator
//! that serves both and enforces boundaries, as Floci-gcp does, named by STORAGE_EMULATOR_HOST, and the service account's
//! key in GOOGLE_SERVICE_ACCOUNT_KEY with its `token_uri` there.

use metalake_iceberg::vend::gcs::Gcs;
use metalake_iceberg::vend::{Access, Minter};

type Failure = Box<dyn std::error::Error>;

/// The token a minter gives for `directory` of bucket `lake`, with `access`.
async fn token(directory: &str, access: Access) -> Result<String, Failure> {
    let minted = Gcs::from_env().mint(&format!("gs://lake/{directory}/"), access).await?;
    Ok(minted.config["gcs.oauth2.token"].clone())
}

#[tokio::test]
async fn rest11_gcs_token_downscoped() -> Result<(), Failure> {
    let Ok(host) = std::env::var("STORAGE_EMULATOR_HOST") else {
        eprintln!("no emulator: STORAGE_EMULATOR_HOST");
        return Ok(());
    };
    let client = reqwest::Client::new();
    // Set up with a token the emulator does not check, as it checks only those it downscoped.
    let any = "Bearer unchecked";
    let made = client
        .post(format!("{host}/storage/v1/b?project=floci-local"))
        .header("authorization", any)
        .json(&serde_json::json!({"name": "lake"}))
        .send()
        .await?;
    assert!(
        made.status().is_success() || made.status() == 409,
        "the bucket: {}",
        made.status()
    );
    let upload = |name: &str, bearer: &str| {
        client
            .post(format!(
                "{host}/upload/storage/v1/b/lake/o?uploadType=media&name={name}"
            ))
            .header("authorization", bearer.to_string())
            .header("content-type", "application/octet-stream")
            .body("rows")
            .send()
    };
    for name in ["db/t/a.parquet", "db/t2/a.parquet"] {
        assert!(upload(name, any).await?.status().is_success(), "{name} written");
    }
    let get = |name: &str, bearer: String| {
        let name = name.replace('/', "%2F");
        client
            .get(format!("{host}/storage/v1/b/lake/o/{name}?alt=media"))
            .header("authorization", bearer)
            .send()
    };

    let read = format!("Bearer {}", token("db/t", Access::Read).await?);
    assert_eq!(
        get("db/t/a.parquet", read.clone()).await?.status(),
        200,
        "its table's object"
    );
    assert_eq!(
        get("db/t2/a.parquet", read.clone()).await?.status(),
        403,
        "another table's"
    );
    assert_eq!(
        upload("db/t/b.parquet", &read).await?.status(),
        403,
        "a reader does not write"
    );

    let write = format!("Bearer {}", token("db/t", Access::ReadWrite).await?);
    assert!(
        upload("db/t/b.parquet", &write).await?.status().is_success(),
        "a writer does"
    );
    assert_eq!(
        upload("db/t2/b.parquet", &write).await?.status(),
        403,
        "not into another table"
    );
    Ok(())
}
