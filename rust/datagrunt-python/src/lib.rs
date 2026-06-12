use pyo3::exceptions::PyValueError;
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

#[pyfunction]
fn normalize_columns(names: Vec<String>) -> Vec<String> {
    datagrunt_core::normalize::normalize_columns(&names)
}

/// Validate that `delimiter` is a single ASCII character and return its byte value.
/// Extracted as a helper so Task 8 (and future tasks) can reuse the same
/// validation without duplicating the error message.
fn delimiter_byte(delimiter: &str) -> PyResult<u8> {
    let mut chars = delimiter.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) if c.is_ascii() => Ok(c as u8),
        _ => Err(PyValueError::new_err(
            "delimiter must be a single ASCII character",
        )),
    }
}

#[pyfunction]
fn infer_delimiter(path: PathBuf) -> PyResult<String> {
    datagrunt_core::delimiter::infer_delimiter(&path).map_err(oserr)
}

#[pyfunction]
fn row_count_with_header(path: PathBuf, delimiter: &str) -> PyResult<u64> {
    datagrunt_core::rows::row_count_with_header(&path, delimiter_byte(delimiter)?).map_err(oserr)
}

#[pyfunction]
fn check_ragged(path: PathBuf, delimiter: &str) -> PyResult<bool> {
    Ok(datagrunt_core::ragged::check_ragged(&path, delimiter_byte(delimiter)?))
}

#[pymodule]
fn datagrunt_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(is_legacy_mac_newlines, m)?)?;
    m.add_function(wrap_pyfunction!(leading_rows, m)?)?;
    m.add_function(wrap_pyfunction!(first_row, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_comments, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_physical_lines_before_header, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_columns, m)?)?;
    m.add_function(wrap_pyfunction!(infer_delimiter, m)?)?;
    m.add_function(wrap_pyfunction!(row_count_with_header, m)?)?;
    m.add_function(wrap_pyfunction!(check_ragged, m)?)?;
    Ok(())
}
