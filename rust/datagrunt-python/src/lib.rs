use pyo3::prelude::*;
use std::path::PathBuf;

/// Map an IO error to a Python exception via PyO3's `From` impl, which
/// preserves errno (e.g. a missing file becomes `FileNotFoundError`,
/// matching the Python implementation).
fn oserr(e: std::io::Error) -> PyErr {
    e.into()
}

#[pyfunction]
fn is_legacy_mac_newlines(path: PathBuf) -> bool {
    datagrunt_core::io::is_legacy_mac_newlines(&path)
}

#[pyfunction]
fn leading_rows(path: PathBuf, limit: usize) -> PyResult<Vec<String>> {
    datagrunt_core::rows::leading_rows(&path, limit).map_err(oserr)
}

#[pyfunction]
fn first_row(path: PathBuf) -> PyResult<String> {
    datagrunt_core::rows::first_row(&path).map_err(oserr)
}

#[pyfunction]
fn count_leading_comments(path: PathBuf) -> PyResult<usize> {
    datagrunt_core::rows::count_leading_comments(&path).map_err(oserr)
}

#[pyfunction]
fn count_leading_physical_lines_before_header(path: PathBuf) -> PyResult<usize> {
    datagrunt_core::rows::count_leading_physical_lines_before_header(&path).map_err(oserr)
}

#[pymodule]
fn datagrunt_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(is_legacy_mac_newlines, m)?)?;
    m.add_function(wrap_pyfunction!(leading_rows, m)?)?;
    m.add_function(wrap_pyfunction!(first_row, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_comments, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_physical_lines_before_header, m)?)?;
    Ok(())
}
