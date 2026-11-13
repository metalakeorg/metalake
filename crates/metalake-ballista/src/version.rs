//! Whether a client and a cluster speak the same plans. Ballista checks its own versions between a client, a scheduler
//! and its executors, and DataFusion's go with them; what it cannot see is the version of MetaLake's encoding of tables,
//! scans and writes. A client says it in its options (`metalake.plan_version`), which reach the scheduler and every
//! executor with each job; each refuses a job of another, with both versions named, instead of failing to read its
//! bytes halfway.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;
use metalake_datafusion::{MetaLakeOptions, PLAN_VERSION};

/// `config`, saying the version of MetaLake's plans this client speaks.
pub(crate) fn said(mut config: SessionConfig) -> SessionConfig {
    let mut options = config.options().extensions.get::<MetaLakeOptions>().cloned().unwrap_or_default();
    options.plan_version = Some(PLAN_VERSION);
    config.options_mut().extensions.insert(options);
    config
}

/// Refuses a job whose client speaks another version of MetaLake's plans than this process, or says none.
pub(crate) fn check(config: &SessionConfig) -> Result<()> {
    let spoken = config.options().extensions.get::<MetaLakeOptions>().and_then(|o| o.plan_version);
    match spoken {
        Some(version) if version == PLAN_VERSION => Ok(()),
        Some(version) => Err(DataFusionError::Configuration(format!(
            "the client speaks MetaLake's plans version {version}, this cluster version {PLAN_VERSION}: use a client \
             and a cluster of matching versions"
        ))),
        None => Err(DataFusionError::Configuration(
            "the client says no version of MetaLake's plans: a MetaLake client says one".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_is_heard_by_its_own_version_only() {
        let config = MetaLakeOptions::register(SessionConfig::new());
        assert!(check(&config).unwrap_err().to_string().contains("says no version"));
        let mut config = said(config);
        assert!(check(&config).is_ok());
        config.options_mut().set("metalake.plan_version", &(PLAN_VERSION + 1).to_string()).unwrap();
        let refused = check(&config).unwrap_err().to_string();
        assert!(refused.contains(&format!("version {}", PLAN_VERSION + 1)), "{refused}");
    }
}
