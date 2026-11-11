//! Credentials for a table's files on Azure (REST-11), against a store that checks them: a SAS minted for a table's
//! directory reads its files and not another table's, and writes them only when minted for a writer; signed with the
//! account's key, or with a key Azure delegates to the server's Entra identity. Needs an emulator that checks both, as
//! Floci-az does, its Blob endpoint in METALAKE_TEST_AZURE_BLOB_ENDPOINT, and, for each test, the server's own store as
//! the minter reads it:
//!
//! - by the account's key: AZURE_STORAGE_USE_EMULATOR=true, which signs with the emulator's account and key;
//! - by an Entra identity: no key, AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ENDPOINT, and the identity's token in
//!   AZURE_STORAGE_TOKEN, which the emulator does not check.

use metalake_iceberg::vend::azure::Azure;
use metalake_iceberg::vend::{Access, Minter};

type Failure = Box<dyn std::error::Error>;

fn set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.is_empty())
}

/// A request to the emulator with no SAS: its shared key is not checked.
fn unchecked(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
        .header("x-ms-version", "2026-10-06")
        .header(
            "x-ms-date",
            chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header("authorization", "SharedKey devstoreaccount1:unchecked")
}

/// The SAS a minter gives for `directory` of container `lake`, with `access`.
async fn sas(directory: &str, access: Access) -> Result<String, Failure> {
    let prefix = format!("abfss://lake@devstoreaccount1.dfs.core.windows.net/{directory}/");
    let minted = Azure::from_env().mint(&prefix, access).await?;
    Ok(minted.config["adls.sas-token.devstoreaccount1.dfs.core.windows.net"].clone())
}

/// What a SAS for `db/t` opens, as the minter in this environment signs it.
async fn narrowed(blobs: &str) -> Result<(), Failure> {
    let client = reqwest::Client::new();
    unchecked(client.put(format!("{blobs}/lake?restype=container")))
        .send()
        .await?;
    for blob in ["db/t/a.parquet", "db/t2/a.parquet"] {
        let put = unchecked(client.put(format!("{blobs}/lake/{blob}")))
            .header("x-ms-blob-type", "BlockBlob")
            .body("rows");
        assert!(put.send().await?.status().is_success(), "{blob} written");
    }
    let get = |blob: &str, sas: &str| client.get(format!("{blobs}/lake/{blob}?{sas}")).send();
    let put = |blob: &str, sas: &str| {
        client
            .put(format!("{blobs}/lake/{blob}?{sas}"))
            .header("x-ms-blob-type", "BlockBlob")
            .header("x-ms-version", "2026-10-06")
            .body("rows")
            .send()
    };

    let read = sas("db/t", Access::Read).await?;
    assert_eq!(get("db/t/a.parquet", &read).await?.status(), 200, "its table's file");
    assert_eq!(get("db/t2/a.parquet", &read).await?.status(), 403, "another table's");
    assert_eq!(
        put("db/t/b.parquet", &read).await?.status(),
        403,
        "a reader does not write"
    );
    let write = sas("db/t", Access::ReadWrite).await?;
    assert_eq!(put("db/t/b.parquet", &write).await?.status(), 201, "a writer does");
    assert_eq!(
        put("db/t2/b.parquet", &write).await?.status(),
        403,
        "not into another table"
    );
    Ok(())
}

#[tokio::test]
async fn rest11_azure_sas_narrowed() -> Result<(), Failure> {
    match std::env::var("METALAKE_TEST_AZURE_BLOB_ENDPOINT") {
        Ok(blobs) if set("AZURE_STORAGE_USE_EMULATOR") => narrowed(&blobs).await,
        _ => {
            eprintln!(
                "no emulator signed for by its key: METALAKE_TEST_AZURE_BLOB_ENDPOINT, AZURE_STORAGE_USE_EMULATOR"
            );
            Ok(())
        }
    }
}

#[tokio::test]
async fn rest11_azure_user_delegation_sas_narrowed() -> Result<(), Failure> {
    match std::env::var("METALAKE_TEST_AZURE_BLOB_ENDPOINT") {
        Ok(blobs) if set("AZURE_STORAGE_TOKEN") => {
            let sas = sas("db/t", Access::Read).await?;
            assert!(sas.contains("skoid="), "signed with a delegated key: {sas}");
            narrowed(&blobs).await
        }
        _ => {
            eprintln!("no emulator reached by an identity: METALAKE_TEST_AZURE_BLOB_ENDPOINT, AZURE_STORAGE_TOKEN");
            Ok(())
        }
    }
}
