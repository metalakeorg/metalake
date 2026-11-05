//! The library of the Python package: one copy of everything Rust, which `import metalake` and the `metalake` command
//! both load, since a wheel with a binary beside it would carry the engine twice.

mod errors;
mod info;
mod lake;
mod result;
mod runtime;
mod table;

use std::ffi::OsString;

use pyo3::prelude::*;

/// Runs a command line, its first argument being the program's name, and answers the code to exit with.
#[pyfunction]
fn cli(py: Python<'_>, args: Vec<OsString>) -> i32 {
    py.detach(|| metalake_cli::main(args))
}

#[pymodule]
fn _metalake(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<lake::PyLake>()?;
    module.add_class::<info::PyRef>()?;
    module.add_class::<info::PyCommit>()?;
    module.add_class::<info::PyDone>()?;
    module.add_class::<info::PyMerged>()?;
    module.add_class::<info::PySnapshot>()?;
    module.add_class::<info::PyStatus>()?;
    module.add_class::<table::PyTable>()?;
    module.add_class::<table::PyWritten>()?;
    module.add_class::<result::QueryResult>()?;
    module.add_class::<result::PyBatch>()?;
    module.add_class::<result::Batches>()?;
    module.add_function(wrap_pyfunction!(lake::connect, module)?)?;
    module.add_function(wrap_pyfunction!(lake::init, module)?)?;
    module.add_function(wrap_pyfunction!(lake::migrate, module)?)?;
    module.add_function(wrap_pyfunction!(lake::stop, module)?)?;
    module.add_function(wrap_pyfunction!(cli, module)?)
}
