from py_data import create_random_tables, create_table_between
import pytest
import pandas as pd
from datetime import datetime


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

@pytest.mark.parametrize(
    "num_tables, start, end",
    [
        (3, datetime(2022, 1, 1), datetime(2022, 1, 5)),
        (1, datetime(2022, 2, 1), datetime(2022, 2, 10)),
        (5, datetime(2023, 3, 1), datetime(2023, 6, 15)),
    ]
)
def test_create_random_tables(num_tables, start, end):
    tables = create_random_tables(num_tables, start, end)

    assert len(tables) == num_tables, f"Expected {num_tables} tables, got {len(tables)}"

    for name, df in tables.items():
        assert name.startswith("Table "), f"Table name '{name}' is not formatted correctly."
        
        assert isinstance(df, pd.DataFrame), f"{name} is not a pandas DataFrame."
        
        assert len(df.columns) > 0, f"{name} has no columns."
        
        assert isinstance(df.index, pd.DatetimeIndex), f"{name} does not have a DateTime index."
        
        assert not df.empty, f"{name} is empty."

def test_empty_table_case():
    tables = create_random_tables(0, datetime(2022, 1, 1), datetime(2022, 1, 5))
 
    assert tables == {}, "Expected an empty dictionary for 0 tables."
