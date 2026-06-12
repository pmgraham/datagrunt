use pyo3::prelude::*;

#[pyfunction]
fn smoke() -> &'static str {
    datagrunt_core::smoke()
}

#[pymodule]
fn datagrunt_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(smoke, m)?)?;
    Ok(())
}
