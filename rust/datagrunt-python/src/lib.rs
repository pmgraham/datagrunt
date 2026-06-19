use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
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

/// CSVDialect equivalent: None for empty/blank files or an undeterminable
/// sample; otherwise the raw sniffed dialect fields. lineterminator and
/// quoting are constants in csv.Sniffer.sniff; escapechar is never set.
///
/// Routes through a single `probe_csv_header` call (1 file open) — the probe's
/// `sample_lines` joined gives the dialect sample, matching the Python refactor
/// in `_compute_python.sniff_dialect`.
#[pyfunction]
#[pyo3(signature = (path, delimiter=None))]
fn sniff_dialect(
    py: Python<'_>,
    path: PathBuf,
    delimiter: Option<String>,
) -> PyResult<Option<Py<PyDict>>> {
    let probe = datagrunt_core::rows::probe_csv_header(&path).map_err(oserr)?;
    if probe.empty || probe.blank {
        return Ok(None);
    }
    let sample = probe.sample_lines.join("");
    // An empty delimiter string is Python-falsy (`if delimiter:`), meaning "no
    // restriction" — normalize it to None so it doesn't reject every candidate
    // (Some("") would make the substring guard `"".contains(x)` reject all).
    let delimiter = delimiter.as_deref().filter(|s| !s.is_empty());
    let Some(d) = datagrunt_core::dialect::sniff(&sample, delimiter) else {
        return Ok(None);
    };
    let dict = PyDict::new(py);
    dict.set_item("delimiter", d.delimiter)?;
    dict.set_item("quotechar", d.quotechar)?;
    dict.set_item("escapechar", py.None())?;
    dict.set_item("doublequote", d.doublequote)?;
    dict.set_item("lineterminator", "\r\n")?;
    dict.set_item("skipinitialspace", d.skipinitialspace)?;
    dict.set_item("quoting", 0)?;
    Ok(Some(dict.into()))
}

/// `probe_csv_header(path) -> dict` — single-pass header probe.
///
/// Returns a dict with keys: `empty`, `blank`, `first_row`, `sample_rows`,
/// `sample_lines`.  Identical values to `_compute_python.probe_csv_header`
/// for all corpus files (enforced by the parity test suite).
#[pyfunction]
fn probe_csv_header(py: Python<'_>, path: PathBuf) -> PyResult<Py<PyDict>> {
    let probe = datagrunt_core::rows::probe_csv_header(&path).map_err(oserr)?;
    let d = PyDict::new(py);
    d.set_item("empty", probe.empty)?;
    d.set_item("blank", probe.blank)?;
    d.set_item("first_row", probe.first_row)?;
    d.set_item("sample_rows", probe.sample_rows)?;
    d.set_item("sample_lines", probe.sample_lines)?;
    Ok(d.into())
}

#[pymodule]
#[pyo3(name = "_native")]
fn datagrunt_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(is_legacy_mac_newlines, m)?)?;
    m.add_function(wrap_pyfunction!(leading_rows, m)?)?;
    m.add_function(wrap_pyfunction!(first_row, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_comments, m)?)?;
    m.add_function(wrap_pyfunction!(count_leading_physical_lines_before_header, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_columns, m)?)?;
    m.add_function(wrap_pyfunction!(infer_delimiter, m)?)?;
    m.add_function(wrap_pyfunction!(row_count_with_header, m)?)?;
    m.add_function(wrap_pyfunction!(check_ragged, m)?)?;
    m.add_function(wrap_pyfunction!(sniff_dialect, m)?)?;
    m.add_function(wrap_pyfunction!(probe_csv_header, m)?)?;
    Ok(())
}
