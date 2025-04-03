use std::collections::HashMap;
use std::sync::{Arc, Mutex};
mod errors;
use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::{ArrowTimestampType, TimestampNanosecondType};
use arrow::pyarrow::FromPyArrow;
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
use pyo3_arrow::PyTable;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

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

        for (key, val) in tables.into_bound(py).iter() {
            let key_str = key.downcast::<PyString>()?.to_str()?.to_owned();

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
            rs_tables.insert(key_str, table);
        }

        Ok(RsCutter { tables: rs_tables })
    }

    #[pyo3(signature = (start=None, end=None, parralel=false))]
    fn slice(
        &self,
        py: Python,
        start: Option<Py<PyDateTime>>,
        end: Option<Py<PyDateTime>>,
        parralel: bool,
    ) -> PyResult<PyObject> {
        let start_ts = RsCutter::parse_py_timestamps(py, start)
            .map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;
        let end_ts = RsCutter::parse_py_timestamps(py, end)
            .map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;

        // DROP GIL -- not really useful in my benchmark cause single threaded python program
        let sliced_tables = py.allow_threads(move || {
            let sliced_tables = Arc::new(Mutex::new(HashMap::new()));
            if parralel {
                self.tables.par_iter().for_each(|(key, val)| {
                    let sliced_rbs = self._slice(start_ts, end_ts, val);
                    let mut sliced_tables_lock = sliced_tables.lock().unwrap();
                    sliced_tables_lock.insert(key.clone(), sliced_rbs);
                });
            } else {
                for (key, value) in self.tables.iter() {
                    let sliced_rbs = self._slice(start_ts, end_ts, value);
                    sliced_tables
                        .lock()
                        .unwrap()
                        .insert(key.clone(), sliced_rbs);
                }
            }
            sliced_tables
        });

        let py_tables = PyDict::new(py);

        // get rid of mutex and take sole ownership

        let sliced_tables_no_arc = Arc::try_unwrap(sliced_tables).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Error un-Arc-ing hashmap: {:?}",
                e
            ))
        })?;

        let sliced_tables_no_mutex = sliced_tables_no_arc.into_inner().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Error un-Mutex-ing hashmap: {:?}",
                e
            ))
        })?;

        for (key, value) in sliced_tables_no_mutex.into_iter() {
            let py_value =
                value.map_err(|e: MyError| PyErr::new::<PyTypeError, _>(format!("{e}")))?;

            let schema = py_value.get(0).unwrap().schema();

            let pa_table = PyTable::try_new(py_value, schema)?.to_pyarrow(py)?;

            py_tables.set_item(key, pa_table)?;
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
        start: Option<i64>,
        end: Option<i64>,
        rbs: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, MyError> {
        if let (Some(start_val), Some(end_val)) = (start, end) {
            if start_val > end_val {
                return Ok(vec![RecordBatch::new_empty(rbs.get(0).unwrap().schema())]);
            }
        }

        let length = RsCutter::get_length(rbs);

        let schema = rbs
            .get(0)
            .ok_or(MyError::IndexError("No record batch found".to_string()))?
            .schema();

        let start_slice = match start {
            Some(s) => self.binary_search_ts(rbs, s, length)?,
            None => 0, // If no start, include everything from the beginning
        };

        let end_slice = match end {
            Some(e) => self.binary_search_ts(rbs, e, length)?,
            None => length, // If no end, include everything until the last row
        };

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

    fn parse_py_timestamps(py: Python, ts: Option<Py<PyDateTime>>) -> Result<Option<i64>, MyError> {
        if ts.is_none() {
            return Ok(None);
        }

        let ts_some = ts.unwrap();

        let bounded_ts = ts_some.into_bound(py);

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

        Ok(Some(TimestampNanosecondType::make_value(date).unwrap()))
    }
}

#[pymodule]
fn rs_cutter(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(to_uppercase, m)?)?;
    m.add_class::<RsCutter>()?;

    Ok(())
}
