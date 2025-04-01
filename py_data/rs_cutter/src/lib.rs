use std::collections::HashMap;

use arrow::{array::RecordBatch, ffi_stream::ArrowArrayStreamReader};
use arrow::pyarrow::FromPyArrow;
use pyo3::{prelude::*, types::{PyDict, PyString}};

/// This function takes a Python string, converts it to uppercase, and returns it.
#[pyfunction]
fn to_uppercase(input: String) -> PyResult<String> {
    Ok(input.to_uppercase())
}

#[pyclass]
struct RsCutter {
    tables: HashMap<String, Vec<RecordBatch>>
}

#[pymethods]
impl RsCutter {
    #[new]
    fn new(py: Python, tables: Py<PyDict>) -> PyResult<Self> {

        let mut rs_tables = HashMap::new();
        println!("Starting to iterate over tables...");

        for (key,val) in tables.into_bound(py).iter() {
            let key_str = key.downcast::<PyString>()?.to_str()?.to_owned();

            println!("Got the string {:?}", key_str);

            let mut reader= ArrowArrayStreamReader::from_pyarrow_bound(&val)?;

            let mut table = vec![];

            while let Some(batch) = reader.next() {
                match batch {
                    Ok(record_batch) => table.push(record_batch),
                    Err(e) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                            format!("Error reading record batch: {:?}", e),
                        ));
                    }
                }
            }

            println!("Got the table");

            rs_tables.insert(key_str, table);

        }

        Ok(RsCutter { tables: rs_tables })

    }

    fn total_row_count(&self) -> usize {
        self.tables
                .values()
                .flat_map(|batches| batches.iter())
                .map(|batch| batch.num_rows())
                .sum()
    }
}

#[pymodule]
fn rs_cutter(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_uppercase, m)?)?;
    m.add_class::<RsCutter>()?;

    Ok(())
}