//! How a verb that serves until stopped learns it is: `metalake iceberg serve` and `metalake ui`.

/// Ctrl-C, or, where there is one, the signal a service manager stops a process with.
pub async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("a signal handler installs");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
