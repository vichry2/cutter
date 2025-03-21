import pytest
import pandas as pd
from datetime import datetime
from py_data import create_table_between


@pytest.mark.parametrize(
    "start, end, freq, columns", [
        (datetime(2022, 1, 1), datetime(2022, 1, 5), 'D', 3),
        (datetime(2022, 1, 1), datetime(2022, 1, 10), '2D', 2),
        (datetime(2022, 1, 1), datetime(2022, 1, 7), 'W', 5),
        (datetime(2022, 1, 1), datetime(2022, 1, 5), 'h', 4),
        (datetime(2022, 2, 1), datetime(2022, 2, 3), 'B', 2),
    ]
)
def test_create_table_between(start, end, freq, columns):

    df = create_table_between(start, end, freq, columns)
    
    expected_num_rows = len(pd.date_range(start=start, end=end, freq=freq))
    assert len(df) == expected_num_rows, f"Expected {expected_num_rows} rows, but got {len(df)}"

    assert df.shape[1] == columns, f"Expected {columns} columns, but got {df.shape[1]}"

    assert pd.api.types.is_datetime64_any_dtype(df.index), "Index should be datetime"

    expected_columns = [f"Column {i+1}" for i in range(columns)]
    assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, but got {list(df.columns)}"
