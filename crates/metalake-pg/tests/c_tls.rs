//! TLS to a Postgres that checks it, as libpq's modes say: `bash sql/test.sh` with `TLS=1` serves a certificate for
//! `metalake-test.invalid` that a CA of the run signs, and lets the role `tls_only` in over TLS alone.

use std::net::ToSocketAddrs;
use std::path::PathBuf;

use metalake_pg::Pool;

const NAME: &str = "metalake-test.invalid";

/// The test server's address, port and the directory of its CAs.
fn server() -> (String, String, PathBuf) {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL is not set: run TLS=1 bash sql/test.sh");
    let tls = std::env::var("METALAKE_TEST_TLS").expect("METALAKE_TEST_TLS is not set: run TLS=1 bash sql/test.sh");
    let value = |key: &str| {
        url.split_whitespace()
            .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
            .unwrap_or_else(|| panic!("DATABASE_URL names no {key}"))
            .to_string()
    };
    let (host, port) = (value("host"), value("port"));
    let address = (host.as_str(), port.parse::<u16>().unwrap())
        .to_socket_addrs()
        .unwrap()
        .find(|a| a.is_ipv4())
        .expect("the host has an IPv4 address")
        .ip()
        .to_string();
    (address, port, PathBuf::from(tls))
}

/// Whether a connection by `postgres` is made, and whether it is encrypted, or why not.
async fn connect(postgres: &str) -> Result<bool, String> {
    let pool = Pool::new(postgres, 1).map_err(|e| e.to_string())?;
    let session = pool.get().await.map_err(|e| format!("{e:?}"))?;
    let row = session
        .query_one("select ssl from pg_stat_ssl where pid = pg_backend_pid()", &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(row.get(0))
}

fn quoted(path: PathBuf) -> String {
    format!("'{}'", path.display().to_string().replace('\\', "\\\\"))
}

#[tokio::test]
async fn verify_full_checks_the_name() {
    let (address, port, tls) = server();
    let ca = quoted(tls.join("ca.pem"));
    let by_name =
        format!("host={NAME} hostaddr={address} port={port} user=postgres sslmode=verify-full sslrootcert={ca}");
    assert_eq!(connect(&by_name).await, Ok(true));

    let by_address = format!("host={address} port={port} user=postgres sslmode=verify-full sslrootcert={ca}");
    let refused = connect(&by_address).await.unwrap_err();
    assert!(refused.contains("NotValidForName"), "{refused}");
}

#[tokio::test]
async fn verify_full_by_url() {
    let (address, port, tls) = server();
    let ca: String = tls
        .join("ca.pem")
        .display()
        .to_string()
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let url =
        format!("postgresql://postgres@{NAME}:{port}/postgres?hostaddr={address}&sslmode=verify-full&sslrootcert={ca}");
    assert_eq!(connect(&url).await, Ok(true));
}

#[tokio::test]
async fn verify_ca_checks_the_chain_alone() {
    let (address, port, tls) = server();
    let base = format!("host={address} port={port} user=postgres sslmode=verify-ca");
    let ca = quoted(tls.join("ca.pem"));
    assert_eq!(
        connect(&format!("{base} sslrootcert={ca}")).await,
        Ok(true),
        "an address is not a name it checks"
    );

    let other = quoted(tls.join("other-ca.pem"));
    let refused = connect(&format!("{base} sslrootcert={other}")).await.unwrap_err();
    assert!(refused.contains("UnknownIssuer"), "{refused}");
}

#[tokio::test]
async fn require_and_prefer_encrypt_unchecked() {
    let (address, port, _) = server();
    let base = format!("host={address} port={port} user=tls_only dbname=postgres");
    assert_eq!(connect(&format!("{base} sslmode=require")).await, Ok(true));
    assert_eq!(connect(&format!("{base} sslmode=prefer")).await, Ok(true));
    assert_eq!(connect(&base).await, Ok(true), "prefer, when no mode is named");

    let refused = connect(&format!("{base} sslmode=disable")).await.unwrap_err();
    assert!(refused.contains("pg_hba.conf"), "{refused}");
}
