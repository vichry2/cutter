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
    freq_options = ["D", "2D", "W", "h", "B"]
    
    for i in range(num_tables):
        cols = random.randint(1, 50)
        freq = random.choice(freq_options)
        res[f"Table {i + 1}"] = create_table_between(start, end, freq, cols)
        
    return res