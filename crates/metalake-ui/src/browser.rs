//! Opening the UI in the user's browser through a redirect file, as Jupyter does (ADR 0007 §6.3): the browser is given a
//! page of the user's own that sends it to the UI's URL, so that what that URL will carry (the run's token, milestone 4)
//! is in no process's arguments, where another user of the machine could read it.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A page that sends the browser to a URL: written readable by its user alone, and removed when dropped, so that
/// leaving `metalake ui` by Ctrl-C or by an error leaves none behind.
#[derive(Debug)]
pub struct Redirect {
    path: PathBuf,
}

impl Redirect {
    /// Writes the page that sends the browser to `url`, in `dir`, as `open-<this process's id>.html`.
    pub fn write(dir: &Path, url: &str) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("open-{}.html", std::process::id()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&path)?;
        file.write_all(page(url).as_bytes())?;
        Ok(Redirect { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Redirect {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Opens `redirect` in the user's default browser.
pub fn open(redirect: &Redirect) -> io::Result<()> {
    let path = redirect.path.canonicalize()?;
    webbrowser::open(&file_url(&path))
}

/// The directory of this user's UI files: `~/.metalake/ui`, beside the Iceberg service's `~/.metalake/iceberg`.
pub fn directory() -> io::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no home directory for MetaLake's UI: set HOME"))?;
    Ok(PathBuf::from(home).join(".metalake").join("ui"))
}

/// The page: a refresh to `url` at once, and a link for a browser that does not follow it.
fn page(url: &str) -> String {
    let url = escape(url);
    format!(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\" />\n\
         <meta http-equiv=\"refresh\" content=\"0;url={url}\" />\n<title>MetaLake</title>\n</head>\n\
         <body>\n<p>Opening <a href=\"{url}\">MetaLake's UI</a>.</p>\n</body>\n</html>\n"
    )
}

/// `text` as an HTML attribute's value.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The `file:` URL of an absolute path, on Windows as elsewhere (RFC 8089): a drive's (`C:\…`, or `\\?\C:\…` as
/// `canonicalize` gives it) as `file:///C:/…`; a network share's (`\\server\share\…`, or `\\?\UNC\server\share\…`), a home
/// on one included, as `file://server/share/…`.
fn file_url(path: &Path) -> String {
    let text = path.to_string_lossy();
    let (share, rest) = if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        (true, rest)
    } else if let Some(rest) = text.strip_prefix(r"\\?\") {
        (false, rest)
    } else if let Some(rest) = text.strip_prefix(r"\\") {
        (true, rest)
    } else {
        (false, text.as_ref())
    };
    let text = rest.replace('\\', "/");
    let mut url = String::from("file://");
    if !share && !text.starts_with('/') {
        url.push('/');
    }
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'-' | b'_' | b'.' | b'~' | b':' => url.push(byte as char),
            _ => url.push_str(&format!("%{byte:02X}")),
        }
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("metalake-ui-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn redirect_written_and_removed() {
        let dir = scratch("redirect");
        let redirect = Redirect::write(&dir, "http://127.0.0.1:1/#token=a&b\"").unwrap();
        let path = redirect.path().to_owned();
        let page = fs::read_to_string(&path).unwrap();
        assert!(
            page.contains(r#"content="0;url=http://127.0.0.1:1/#token=a&amp;b&quot;""#),
            "{page}"
        );
        assert!(
            page.contains(r#"<a href="http://127.0.0.1:1/#token=a&amp;b&quot;">"#),
            "{page}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "readable by its user alone");
        }
        drop(redirect);
        assert!(!path.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_url_of_paths() {
        assert_eq!(
            file_url(Path::new(r"\\?\C:\Users\PC\.metalake\ui\open-1.html")),
            "file:///C:/Users/PC/.metalake/ui/open-1.html"
        );
        assert_eq!(file_url(Path::new(r"C:\a b\ñ.html")), "file:///C:/a%20b/%C3%B1.html");
        // A home on a network share, as `canonicalize` gives it and as it is written.
        assert_eq!(
            file_url(Path::new(r"\\?\UNC\server\profiles\alice\.metalake\ui\open-1.html")),
            "file://server/profiles/alice/.metalake/ui/open-1.html"
        );
        assert_eq!(
            file_url(Path::new(r"\\server\profiles\alice\open-1.html")),
            "file://server/profiles/alice/open-1.html"
        );
        assert_eq!(
            file_url(Path::new("/home/a b/open-1.html")),
            "file:///home/a%20b/open-1.html"
        );
    }
}
