//! Azure Database for PostgreSQL's token: an Entra access token of the client's identity for the resource
//! `https://ossrdbms-aad.database.windows.net`, sent as the password over TLS. The identity, in the order Azure's own
//! `DefaultAzureCredential` tries them: a service principal's secret (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
//! `AZURE_CLIENT_SECRET`); a workload identity's federated token (`AZURE_FEDERATED_TOKEN_FILE`); a managed identity,
//! an App Service's (`IDENTITY_ENDPOINT`) or an instance's (IMDS); and last the Azure CLI's signed-in user.
//! <https://learn.microsoft.com/azure/postgresql/flexible-server/how-to-configure-sign-in-azure-ad-authentication>

use std::time::Duration;

use futures_util::future::BoxFuture;
use metalake_pg::{Credential, Password};

use super::{error, http};

const RESOURCE: &str = "https://ossrdbms-aad.database.windows.net";
const AUTHORITY: &str = "https://login.microsoftonline.com";
const IMDS: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
/// How long an instance's metadata service may take to answer before the CLI is asked instead.
const IMDS_WAIT: Duration = Duration::from_secs(2);

/// Where the client's identity comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Secret {
        tenant: String,
        client: String,
        secret: String,
    },
    Federated {
        tenant: String,
        client: String,
        file: String,
    },
    AppService {
        endpoint: String,
        header: String,
        client: Option<String>,
    },
    /// An instance's metadata service, then the Azure CLI.
    Instance { client: Option<String> },
}

#[derive(Debug)]
pub(crate) struct Azure {
    source: Source,
    authority: String,
    imds: String,
}

impl Azure {
    pub(crate) fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let (tenant, client) = (var("AZURE_TENANT_ID"), var("AZURE_CLIENT_ID"));
        let source = match (
            tenant,
            client.clone(),
            var("AZURE_CLIENT_SECRET"),
            var("AZURE_FEDERATED_TOKEN_FILE"),
        ) {
            (Some(tenant), Some(client), Some(secret), _) => Source::Secret { tenant, client, secret },
            (Some(tenant), Some(client), None, Some(file)) => Source::Federated { tenant, client, file },
            _ => match (var("IDENTITY_ENDPOINT"), var("IDENTITY_HEADER")) {
                (Some(endpoint), Some(header)) => Source::AppService {
                    endpoint,
                    header,
                    client,
                },
                _ => Source::Instance { client },
            },
        };
        Self {
            source,
            authority: var("AZURE_AUTHORITY_HOST").unwrap_or_else(|| AUTHORITY.into()),
            imds: IMDS.into(),
        }
    }

    async fn mint(&self) -> metalake_pg::Result<Password> {
        let client = http::client();
        let scope = format!("{RESOURCE}/.default");
        match &self.source {
            Source::Secret {
                tenant,
                client: id,
                secret,
            } => {
                let form = [
                    ("grant_type", "client_credentials"),
                    ("client_id", id.as_str()),
                    ("client_secret", secret.as_str()),
                    ("scope", scope.as_str()),
                ];
                self.entra(&client, tenant, &form).await
            }
            Source::Federated {
                tenant,
                client: id,
                file,
            } => {
                let assertion = std::fs::read_to_string(file)
                    .map_err(|e| error(format!("azure-entra: the federated token {file}: {e}")))?;
                let form = [
                    ("grant_type", "client_credentials"),
                    ("client_id", id.as_str()),
                    (
                        "client_assertion_type",
                        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                    ),
                    ("client_assertion", assertion.trim()),
                    ("scope", scope.as_str()),
                ];
                self.entra(&client, tenant, &form).await
            }
            Source::AppService {
                endpoint,
                header,
                client: id,
            } => {
                let mut query = vec![("api-version", "2019-08-01"), ("resource", RESOURCE)];
                if let Some(id) = id {
                    query.push(("client_id", id));
                }
                let request = client.get(endpoint).query(&query).header("X-IDENTITY-HEADER", header);
                let what = "azure-entra: the App Service's managed identity";
                http::token(&http::json(request, what).await?, what)
            }
            Source::Instance { client: id } => {
                let mut query = vec![("api-version", "2018-02-01"), ("resource", RESOURCE)];
                if let Some(id) = id {
                    query.push(("client_id", id));
                }
                let request = client
                    .get(&self.imds)
                    .query(&query)
                    .header("Metadata", "true")
                    .timeout(IMDS_WAIT);
                let what = "azure-entra: the instance's managed identity";
                match http::json(request, what).await {
                    Ok(body) => http::token(&body, what),
                    Err(imds) => cli()
                        .await
                        .map_err(|cli| error(format!("azure-entra: no identity: {imds}; and the Azure CLI: {cli}"))),
                }
            }
        }
    }

    async fn entra(
        &self,
        client: &reqwest::Client,
        tenant: &str,
        form: &[(&str, &str)],
    ) -> metalake_pg::Result<Password> {
        let url = format!("{}/{tenant}/oauth2/v2.0/token", self.authority.trim_end_matches('/'));
        let what = "azure-entra: Entra's token service";
        http::token(&http::json(client.post(&url).form(form), what).await?, what)
    }
}

impl Credential for Azure {
    fn password(&self) -> BoxFuture<'_, metalake_pg::Result<Password>> {
        Box::pin(self.mint())
    }
}

/// The Azure CLI's token for its signed-in user.
async fn cli() -> Result<Password, String> {
    let output = tokio::task::spawn_blocking(|| {
        let args = [
            "account",
            "get-access-token",
            "--resource",
            RESOURCE,
            "--output",
            "json",
        ];
        if cfg!(windows) {
            std::process::Command::new("cmd").args(["/C", "az"]).args(args).output()
        } else {
            std::process::Command::new("az").args(args).output()
        }
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| format!("az: {e}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| format!("az's answer: {e}"))?;
    let token = serde_json::json!({
        "access_token": body["accessToken"],
        "expires_on": body["expires_on"],
    });
    http::token(&token, "az").map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(source: Source, authority: &str) -> Azure {
        Azure {
            source,
            authority: authority.into(),
            imds: "http://127.0.0.1:9".into(),
        }
    }

    #[tokio::test]
    async fn secret_exchanged_for_postgres() {
        let (address, got) = http::tests::answering(200, r#"{"access_token":"eyJ","expires_in":3599}"#);
        let source = Source::Secret {
            tenant: "t1".into(),
            client: "c1".into(),
            secret: "s".into(),
        };
        assert_eq!(with(source, &address).mint().await.unwrap().value, "eyJ");
        let request = got.recv().unwrap();
        assert!(request.starts_with("POST /t1/oauth2/v2.0/token "), "{request}");
        assert!(
            request.contains("scope=https%3A%2F%2Fossrdbms-aad.database.windows.net%2F.default"),
            "{request}"
        );
        assert!(request.contains("grant_type=client_credentials&client_id=c1&client_secret=s"));
    }

    #[tokio::test]
    async fn federated_token_sent_as_assertion() {
        let file = std::env::temp_dir().join(format!("metalake-federated-{}", std::process::id()));
        std::fs::write(&file, "k8s.token\n").unwrap();
        let (address, got) = http::tests::answering(200, r#"{"access_token":"f","expires_in":60}"#);
        let source = Source::Federated {
            tenant: "t".into(),
            client: "c".into(),
            file: file.display().to_string(),
        };
        assert_eq!(with(source, &address).mint().await.unwrap().value, "f");
        assert!(got.recv().unwrap().contains("client_assertion=k8s.token&"));
        std::fs::remove_file(file).unwrap();
    }

    #[tokio::test]
    async fn app_service_identity() {
        let (address, got) = http::tests::answering(200, r#"{"access_token":"a","expires_on":"1"}"#);
        let source = Source::AppService {
            endpoint: format!("{address}/msi/token"),
            header: "h".into(),
            client: Some("c".into()),
        };
        assert_eq!(with(source, AUTHORITY).mint().await.unwrap().value, "a");
        let request = got.recv().unwrap().to_ascii_lowercase();
        assert!(
            request.contains("/msi/token?api-version=2019-08-01&resource=https%3a%2f%2fossrdbms-aad"),
            "{request}"
        );
        assert!(request.contains("x-identity-header: h"));
    }
}
