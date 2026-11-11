fn main() {
    // The lake's Iceberg server runs as this same command: a client that starts one runs this executable.
    if let Ok(executable) = std::env::current_exe() {
        metalake_client::server::set_launcher(vec![executable.into_os_string()]);
    }
    std::process::exit(metalake_cli::main(std::env::args_os()));
}
