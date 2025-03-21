import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any

def create_table_between(start: datetime, end: datetime, freq: str, columns: int) -> pd.DataFrame:
    date_range = pd.date_range(start=start, end=end, freq=freq)
    
    data = np.random.randint(1, 100, size=(len(date_range), columns))
    
    column_names = [f"Column {i + 1}" for i in range(columns)]
    
    df = pd.DataFrame(data, columns=column_names, index=date_range)
    
    return df