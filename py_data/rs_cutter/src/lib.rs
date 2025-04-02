use std::collections::HashMap;
mod errors;
use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::{ArrowTimestampType, TimestampNanosecondType};
use arrow::pyarrow::FromPyArrow;
use arrow::pyarrow::ToPyArrow;
use arrow::{array::RecordBatch, ffi_stream::ArrowArrayStreamReader};
use chrono::NaiveDate;
use errors::MyError;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyDateAccess, PyDateTime, PyTimeAccess};
use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};

/// This function takes a Python string, converts it to uppercase, and returns it.
#[pyfunction]
fn to_uppercase(input: String) -> PyResult<String> {
    Ok(input.to_uppercase())
}

#[pyclass]
struct RsCutter {
    tables: HashMap<String, Vec<RecordBatch>>,
}

#[pymethods]
impl RsCutter {
    #[new]
    fn new(py: Python, tables: Py<PyDict>) -> PyResult<Self> {
        let mut rs_tables = HashMap::new();
        println!("Starting to iterate over tables...");

        for (key, val) in tables.into_bound(py).iter() {
            let key_str = key.downcast::<PyString>()?.to_str()?.to_owned();

            println!("Got the string {:?}", key_str);

            let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(&val)?;

            let mut table = vec![];

            while let Some(batch) = reader.next() {
                match batch {
                    Ok(record_batch) => table.push(record_batch),
                    Err(e) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                            "Error reading record batch: {:?}",
                            e
                        )));
                    }
                }
            }

            println!("Got the table");

            rs_tables.insert(key_str, table);
        }

        Ok(RsCutter { tables: rs_tables })
    }

    fn slice(
        &self,
        py: Python,
        start_date: Py<PyDateTime>,
        end_date: Py<PyDateTime>,
    ) -> PyResult<PyObject> {
        let start_ts = RsCutter::parse_py_timestamps(py, start_date)
            .map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;
        let end_ts = RsCutter::parse_py_timestamps(py, end_date)
            .map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;

        let mut sliced_tables = HashMap::new();

        for (key, value) in self.tables.iter() {
            let sliced_rbs = self._slice(start_ts, end_ts, value);
            sliced_tables.insert(key, sliced_rbs);
        }

        let py_tables = PyDict::new(py);

        for (key, value) in sliced_tables.into_iter() {
            let py_value =
                value.map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;

            let py_vec: Vec<Py<PyAny>> = py_value
                .iter()
                .map(|batch| batch.to_pyarrow(py))
                .collect::<Result<_, _>>()?;

            py_tables.set_item(key, py_vec)?;
        }

        let test = py_tables.into_py_any(py)?;

        Ok(test)
    }

    fn total_row_count(&self) -> usize {
        self.tables
            .values()
            .flat_map(|batches| batches.iter())
            .map(|batch| batch.num_rows())
            .sum()
    }
}

impl RsCutter {
    fn _slice(
        &self,
        start: i64,
        end: i64,
        rbs: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, MyError> {
        let length = RsCutter::get_length(rbs);

        let schema = rbs
            .get(0)
            .ok_or(MyError::IndexError("No record batch found".to_string()))?
            .schema();

        let start_slice = self.binary_search_ts(rbs, start, length)?;
        let end_slice = self.binary_search_ts(rbs, end, length)?;

        if start_slice >= length || end_slice <= 0 {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let mut sliced_rbs = vec![];
        let mut cum_start_index = 0;

        let mut contains_start = false;
        let mut contains_end = false;
        let mut started = false;

        for batch in rbs {
            if start_slice >= cum_start_index && start_slice < cum_start_index + batch.num_rows() {
                contains_start = true;
            }

            if end_slice >= cum_start_index && start_slice < cum_start_index + batch.num_rows() {
                contains_end = true;
            }

            if started && !contains_end {
                sliced_rbs.push(batch.slice(0, batch.num_rows()));
            }
            // Case when both start and end are in this batch
            else if contains_start && contains_end && !started {
                let start_index = start_slice - cum_start_index;
                let end_index = end_slice - cum_start_index;
                sliced_rbs.push(batch.slice(start_index, end_index - start_index));
                break;
            }
            // Case when only start is in this batch
            else if contains_start && !started {
                let start_index = start_slice - cum_start_index;
                sliced_rbs.push(batch.slice(start_index, batch.num_rows() - start_index));
                started = true;
            }
            // Case when only end is in this batch and we already started slicing
            else if contains_end && started {
                let end_index = end_slice - cum_start_index;
                sliced_rbs.push(batch.slice(0, end_index));
                break;
            }

            cum_start_index += batch.num_rows();
        }

        Ok(sliced_rbs)
    }

    fn get_ts(&self, batches: &[RecordBatch], index: usize) -> Result<i64, MyError> {
        let mut start_index;
        let mut end_index = 0;

        for batch in batches {
            start_index = end_index;
            end_index = batch.num_rows() + start_index;

            if index >= start_index && index < end_index {
                // Compute the row index within this batch
                let local_index = index - start_index;

                // Assume the TS column is at index 0 (update if necessary)
                let ts_column = batch.column(0);

                if let Some(ts_array) = ts_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                {
                    if let Some(ts_value) = ts_array.value(local_index).into() {
                        return Ok(ts_value);
                    } else {
                        return Err(MyError::IndexError(format!(
                            "Timestamp at index {index} is null"
                        )));
                    }
                } else {
                    return Err(MyError::ColumnTypeError(
                        "TS column is not a TimestampNanosecondArray".to_string(),
                    ));
                }
            }
        }

        return Err(MyError::IndexError(format!("Index {index} out of bounds")));
    }

    fn get_length(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn binary_search_ts(
        &self,
        rbs: &[RecordBatch],
        target_ts: i64,
        total_rows: usize,
    ) -> Result<usize, MyError> {
        let mut start_index = 0;
        let mut end_index = total_rows - 1;

        while start_index <= end_index {
            let mid_index = (start_index + end_index) / 2;

            // Get the timestamp at mid_index across all batches using get_ts
            let ts = self.get_ts(rbs, mid_index)?;

            // Compare the timestamp to the target
            if ts < target_ts {
                start_index = mid_index + 1;
            } else if ts > target_ts {
                end_index = mid_index - 1;
            } else {
                return Ok(mid_index);
            }
        }

        // Return the index where the timestamp should be inserted or found
        Ok(start_index)
    }

    fn parse_py_timestamps(py: Python, ts: Py<PyDateTime>) -> Result<i64, MyError> {
        let bounded_ts = ts.into_bound(py);

        let year = bounded_ts.get_year();
        let month = bounded_ts.get_month();
        let day = bounded_ts.get_day();
        let hour = bounded_ts.get_hour();
        let minute = bounded_ts.get_minute();
        let second = bounded_ts.get_second();

        let naive_date = NaiveDate::from_ymd_opt(year, month.into(), day.into()).ok_or(
            MyError::DateError("Could not parse year, month, day".to_string()),
        )?;

        let date = naive_date
            .and_hms_nano_opt(hour.into(), minute.into(), second.into(), 0)
            .ok_or(MyError::DateError(
                "Could not parse hour, minute, seconds, nano".to_string(),
            ))?;

        Ok(TimestampNanosecondType::make_value(date).unwrap())
    }
}

#[pymodule]
fn rs_cutter(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_uppercase, m)?)?;
    m.add_class::<RsCutter>()?;

    Ok(())
}
