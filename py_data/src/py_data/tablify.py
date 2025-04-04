import random
import pandas as pd
import pyarrow as pa
import numpy as np
from datetime import datetime
from typing import Dict

def create_table_between(start: datetime, end: datetime, freq: str, columns: int) -> pa.Table:
        
    date_range = pd.date_range(start=start, end=end, freq=freq)
    
    data = np.random.randint(1, 100, size=(len(date_range), columns))
    
    column_names = [f"Column {i + 1}" for i in range(columns)]
    
    df = pd.DataFrame(data, columns=column_names)
    
    df.insert(0, "TS", date_range)
    
    return pa.Table.from_pandas(df)

def create_random_tables(num_tables: int, start: datetime, end: datetime) -> Dict[str, pa.Table]:
    
    res = dict()
    freq_options = ["D", "h", "B"]
    
    for i in range(num_tables):
        cols = random.randint(1, 50)
        freq = random.choice(freq_options)
        res[f"Table {i + 1}"] = create_table_between(start, end, freq, cols)
        
    return res

def create_single_table(num_rows: int, num_columns: int, start: datetime = datetime(2020, 1, 1), table_name : str = "Table 1") -> Dict[str, pa.Table]:
    freq = "min"
    
    date_range = pd.date_range(start=start, periods=num_rows, freq=freq)
    
    data = np.random.randint(1, 100, size=(num_rows, num_columns))
    
    column_names = [f"Column {i + 1}" for i in range(num_columns)]
    
    df = pd.DataFrame(data, columns=column_names)
    df.insert(0, "TS", date_range)
    
    table = pa.Table.from_pandas(df)
    
    return {table_name: table}

def create_multiple_tables(num_tables: int, num_rows_per_table: int, num_columns_per_table: int, start: datetime = datetime(2020, 1, 1)) -> Dict[str, pa.Table]:
    all_tables = {}

    for i in range(num_tables):
        table_name = f"Table {i+1}"

        curr_table = create_single_table(num_rows_per_table, num_columns_per_table, start, table_name)

        all_tables[table_name] = curr_table.get(table_name)

    return all_tables
