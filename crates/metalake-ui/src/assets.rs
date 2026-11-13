//! The frontend's files, as the router reads them: behind [`Files`], so that what serves them does not depend on whether
//! `pnpm build` ran before `cargo build`. [`Dist`] is what the binary embeds; a test gives files of its own.

use std::borrow::Cow;

use rust_embed::RustEmbed;

/// The application's page, which every route of the frontend's own router answers with.
pub(crate) const INDEX: &str = "index.html";

/// One file of the frontend, by its path under `web/dist/`.
#[derive(Debug, Clone)]
pub(crate) struct File {
    pub bytes: Cow<'static, [u8]>,
    /// Its media type, by its extension.
    pub media_type: Cow<'static, str>,
    /// The SHA-256 of its bytes, which an entity tag names (ADR 0007 §4.3).
    pub sha256: [u8; 32],
}

/// Where the frontend's files come from.
pub(crate) trait Files: Send + Sync + 'static {
    /// The file at `path`, relative to `web/dist/` and without a leading `/`.
    fn get(&self, path: &str) -> Option<File>;

    /// Whether the frontend was built: whether its page is there.
    fn built(&self) -> bool {
        self.get(INDEX).is_some()
    }
}

/// What `pnpm build` wrote to `web/dist/` when the crate was compiled, embedded in the binary. The `.gitkeep` keeps the
/// folder, so the crate compiles with no frontend built, and then embeds nothing. The Notebook's extension, built beside
/// it, is not the UI's to serve: [`Selector`] embeds it.
#[derive(RustEmbed)]
#[folder = "web/dist/"]
#[exclude = ".gitkeep"]
#[exclude = "labextension/**"]
pub(crate) struct Dist;

/// The Notebook's ref selector (ADR 0008 §6), the JupyterLab extension `jupyter/` builds into `web/dist/labextension/`,
/// embedded so that each run of JupyterLab is given it (`notebook::kernel`). None if it was not built.
#[derive(RustEmbed)]
#[folder = "web/dist/labextension/"]
#[allow_missing = true]
pub(crate) struct Selector;

impl Files for Dist {
    fn get(&self, path: &str) -> Option<File> {
        let file = <Dist as RustEmbed>::get(path)?;
        Some(File {
            media_type: Cow::Owned(file.metadata.mimetype().to_owned()),
            sha256: file.metadata.sha256_hash(),
            bytes: file.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Dist, Files, Selector};

    /// The Notebook's extension is built beside the UI's files, but only the Notebook is given it.
    #[test]
    fn selector_apart_from_ui() {
        assert!(Dist.get("labextension/package.json").is_none());
        if let Some(package) = <Selector as rust_embed::RustEmbed>::get("package.json") {
            let package: serde_json::Value = serde_json::from_slice(&package.data).unwrap();
            assert_eq!(package["name"], crate::notebook::SELECTOR);
        }
    }
}
