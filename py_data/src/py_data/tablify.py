import random
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any, Dict

def create_table_between(start: datetime, end: datetime, freq: str, columns: int) -> pd.DataFrame:
    date_range = pd.date_range(start=start, end=end, freq=freq)
    
    data = np.random.randint(1, 100, size=(len(date_range), columns))
    
    column_names = [f"Column {i + 1}" for i in range(columns)]
    
    df = pd.DataFrame(data, columns=column_names, index=date_range)
    
    return df

def create_random_tables(num_tables: int, start: datetime, end: datetime) -> Dict[str, pd.DataFrame]:
    
    res = dict()
    freq_options = ["D", "2D", "W", "h", "B"]
    
    for i in range(num_tables):
        cols = random.randint(1, 50)
        freq = random.choice(freq_options)
        res[f"Table {i + 1}"] = create_table_between(start, end, freq, cols)
        
    return res