use pyo3::prelude::*;

/// This function takes a Python string, converts it to uppercase, and returns it.
#[pyfunction]
fn to_uppercase(input: String) -> PyResult<String> {
    Ok(input.to_uppercase())
}

#[pymodule]
fn rs_cutter(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_uppercase, m)?)?;

    Ok(())
}