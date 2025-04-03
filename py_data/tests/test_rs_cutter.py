from py_data.tablify import create_single_table
from rs_cutter import RsCutter
import pandas as pd
import pytest
from datetime import datetime
from py_data.tablify import create_random_tables
from py_data.cutter import Cutter

@pytest.fixture
def start():
    return datetime(2023, 1, 1)

@pytest.fixture
def end():
    return datetime(2023, 2, 10)

@pytest.fixture
def tables(start, end):
    return create_random_tables(num_tables=3, start=start, end=end)

@pytest.fixture
def cutter(tables):
    return RsCutter(tables)

# Test slicing between a range of dates
def test_slice_between_dates(cutter):
    start_date = datetime(2023, 1, 3)
    end_date = datetime(2023, 1, 7)

    sliced_tables = cutter.slice(start=start_date, end=end_date)

    for table_name, table in sliced_tables.items():
        df = table.to_pandas()
        print(table)
        assert df['TS'].min() >= start_date
        assert df['TS'].max() <= end_date

# Test slicing from the start to the given end date
def test_slice_to_end(cutter, start):
    end_date = datetime(2023, 1, 7)

    sliced_tables = cutter.slice(end=end_date)

    for table_name, table in sliced_tables.items():
        df = table.to_pandas()
        assert df['TS'].min() >= start
        assert df['TS'].max() <= end_date

# Test slicing from a given start date to the end
def test_slice_from_start(cutter, end):
    start_date = datetime(2023, 1, 5)

    sliced_tables = cutter.slice(start=start_date)

    for table_name, table in sliced_tables.items():
        df = table.to_pandas()
        assert df['TS'].min() >= start_date
        assert df['TS'].max() <= end

# Test slicing with no start or end date (should return all tables unchanged)
def test_slice_with_no_dates(cutter, tables):
    sliced_tables = cutter.slice()

    for table_name, table in sliced_tables.items():
        original_df = tables[table_name].to_pandas()
        sliced_df = table.to_pandas()
        
        # Ensure the tables are identical (same data before and after slicing)
        pd.testing.assert_frame_equal(original_df, sliced_df)

# Test edge case: slicing when there is no data within the range
def test_slice_no_data_in_range(cutter):
    start_date = datetime(2023, 3, 1)  # Date outside the range of tables
    end_date = datetime(2023, 3, 10)

    sliced_tables = cutter.slice(start=start_date, end=end_date)

    for table_name, table in sliced_tables.items():
        df = table.to_pandas()
        assert df.empty  # No data should be in the result as all dates are outside the range

# Test slicing with invalid range: start > end
def test_invalid_range(cutter):
    start_date = datetime(2023, 1, 7)
    end_date = datetime(2023, 1, 5)  # Invalid range (start > end)

    sliced_tables = cutter.slice(start=start_date, end=end_date)

    for table_name, table in sliced_tables.items():
        df = table.to_pandas()
        assert df.empty  # The result should be empty tables since the range is invalid
        
# Test that an empty cutter raises an exception
def test_empty_cutter():
    with pytest.raises(ValueError):
        Cutter({})  # Should raise ValueError since no tables are provided



def test_rs_cutter_init():
    table = create_single_table(100, 5)

    cutter = RsCutter(table)

    assert cutter.total_row_count() == 100