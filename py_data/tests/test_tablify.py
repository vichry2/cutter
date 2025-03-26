from py_data.tablify import create_random_tables, create_table_between
import pytest
import pyarrow as pa
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
    table = create_table_between(start, end, freq, columns)

    assert isinstance(table, pa.Table), "Return type should be a PyArrow Table"

    expected_num_rows = len(pd.date_range(start=start, end=end, freq=freq))
    assert table.num_rows == expected_num_rows, f"Expected {expected_num_rows} rows, but got {table.num_rows}"

    assert table.num_columns - 1 == columns, f"Expected {columns} columns, but got {table.num_columns}"

    expected_columns = [f"Column {i+1}" for i in range(columns)]
    expected_columns.insert(0, "TS")
    
    assert table.column_names == expected_columns, f"Expected columns {expected_columns}, but got {table.column_names}"


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

    for name, table in tables.items():
        assert name.startswith("Table "), f"Table name '{name}' is not formatted correctly."
        
        assert isinstance(table, pa.Table), f"{name} is not a PyArrow Table."

        assert table.num_columns > 0, f"{name} has no columns."

        assert table.num_rows > 0, f"{name} is empty."


def test_empty_table_case():
    tables = create_random_tables(0, datetime(2022, 1, 1), datetime(2022, 1, 5))

    assert tables == {}, "Expected an empty dictionary for 0 tables."
