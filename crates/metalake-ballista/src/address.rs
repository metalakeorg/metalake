//! A scheduler's address, as a client and an executor are given it: `df://host:port`, `host:port` or a host alone, an
//! IPv6 host in brackets (`df://[::1]:50050`). Ballista's port when it names none.

use datafusion::error::{DataFusionError, Result};

/// Ballista's scheduler port.
pub(crate) const DEFAULT_PORT: u16 = 50050;

/// The host and port of `url`. An IPv6 host keeps its brackets, as a URL or a socket address writes it.
pub(crate) fn host_port(url: &str) -> Result<(String, u16)> {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.strip_prefix('[') {
        // An IPv6 host: the port, if any, follows its closing bracket.
        Some(inside) => {
            let (address, after) = inside
                .split_once(']')
                .ok_or_else(|| invalid(url, "an IPv6 host without its closing bracket"))?;
            let port = match after {
                "" => None,
                after => Some(after.strip_prefix(':').ok_or_else(|| invalid(url, "text after the IPv6 host"))?),
            };
            (format!("[{address}]"), port)
        }
        None => match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), Some(port)),
            None => (authority.to_string(), None),
        },
    };
    if host.is_empty() || host == "[]" {
        return Err(invalid(url, "no host"));
    }
    let port = match port {
        Some(port) => port.parse().map_err(|e| invalid(url, &format!("port {port}: {e}")))?,
        None => DEFAULT_PORT,
    };
    Ok((host, port))
}

fn invalid(url: &str, why: &str) -> DataFusionError {
    DataFusionError::Configuration(format!("scheduler address {url}: {why}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_ports_and_brackets() {
        let at = |url| host_port(url).unwrap();
        assert_eq!(at("df://scheduler:50060"), ("scheduler".into(), 50060));
        assert_eq!(at("df://scheduler"), ("scheduler".into(), DEFAULT_PORT));
        assert_eq!(at("10.0.0.5:7000"), ("10.0.0.5".into(), 7000));
        assert_eq!(at("df://[::1]"), ("[::1]".into(), DEFAULT_PORT));
        assert_eq!(at("df://[::1]:7000"), ("[::1]".into(), 7000));
        assert_eq!(at("df://[fd00::5]:7000/x"), ("[fd00::5]".into(), 7000));
        for wrong in ["df://scheduler:port", "df://[::1", "df://[::1]x", "df://:7000", "df://[]"] {
            assert!(host_port(wrong).is_err(), "{wrong}");
        }
    }
}
