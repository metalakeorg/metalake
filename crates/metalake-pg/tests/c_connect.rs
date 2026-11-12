//! The connection to the catalog's Postgres: TLS to a server that checks it, as libpq's modes say, and a password
//! per connection from a credential. `bash sql/test.sh` with `TLS=1` serves a certificate for `metalake-test.invalid`
//! that a CA of the run signs, lets the role `tls_only` in over TLS alone and `by_password` by its password alone.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use metalake_pg::{Credential, Error, ErrorKind, Password, Pool};

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

/// A credential that answers the passwords it is given, in turn, each already expired so that each connection asks.
#[derive(Debug, Default)]
struct Turns(Mutex<Vec<Result<String, String>>>);

impl Turns {
    fn next(&self, answer: Result<&str, &str>) {
        self.0
            .lock()
            .unwrap()
            .push(answer.map(str::to_string).map_err(str::to_string));
    }
}

impl Credential for Turns {
    fn password(&self) -> BoxFuture<'_, metalake_pg::Result<Password>> {
        Box::pin(async move {
            let answer = self.0.lock().unwrap().remove(0);
            answer
                .map(|value| Password {
                    value,
                    expires: Some(Instant::now()),
                })
                .map_err(|e| Error::new(ErrorKind::Postgres, e))
        })
    }
}

async fn admin(sql: &str) {
    let url = std::env::var("DATABASE_URL").unwrap();
    let pool = Pool::new(&url, 1).unwrap();
    pool.get().await.unwrap().batch_execute(sql).await.unwrap();
}

#[tokio::test]
async fn each_connection_opens_with_the_password_of_its_time() {
    let (address, port, _) = server();
    let turns = Arc::new(Turns::default());
    let url = format!("host={address} port={port} user=by_password dbname=postgres sslmode=require password=ignored");
    let pool = Pool::connecting(&url, 3, Duration::from_secs(10), Some(turns.clone())).unwrap();

    admin("alter role by_password password 'one'").await;
    turns.next(Ok("one"));
    let first = pool.get().await.expect("the credential's password, not the string's");

    admin("alter role by_password password 'two'").await;
    turns.next(Ok("two"));
    let second = pool.get().await.expect("a new connection asks again");
    first
        .simple_query("select 1")
        .await
        .expect("an open connection outlives its password");

    turns.next(Ok("three"));
    let refused = pool.get().await.unwrap_err();
    assert!(
        format!("{refused:?}").contains("password authentication failed"),
        "{refused:?}"
    );

    turns.next(Err("no identity"));
    let refused = pool.get().await.unwrap_err();
    assert!(refused.to_string().contains("the catalog's credential"), "{refused}");

    drop(first);
    turns.next(Err("no identity still"));
    let idle = pool
        .get()
        .await
        .expect("an open connection is given while the credential fails");
    idle.simple_query("select 1").await.unwrap();
    drop((idle, second));
}
