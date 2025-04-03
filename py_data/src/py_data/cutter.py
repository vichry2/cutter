from typing import Dict
import pyarrow as pa
import pandas as pd
import numpy as np
from datetime import datetime

class Cutter:
    tables: Dict[str, pa.Table]
    
    def __init__(self, tables: Dict[str, pa.Table]):
        if not tables:
            raise ValueError("You must provide a dictionary of tables.")
        self.tables = tables
        
    def slice(self, start: datetime | None = None, end: datetime | None = None) -> Dict[str, pa.Table]:
        return {name: self._slice(table, start, end) for name, table in self.tables.items()}
    
    def _slice(self, table: pa.Table, start: datetime | None, end: datetime | None) -> pa.Table:
        """
        Slices a given table based on the start and end dates using pyarrow's slice method.
        """
        # Convert the pyarrow Table to pandas for date access
        df = table.to_pandas()

        # Ensure 'TS' column is in datetime format
        df['TS'] = pd.to_datetime(df['TS'])

        # If no start or end date is provided, return the full table
        if start is None and end is None:
            return table
        
        # Convert start and end to np.datetime64 for comparison
        ts_sorted = df['TS'].values
        if start is not None:
            start_np = np.datetime64(start)
        if end is not None:
            end_np = np.datetime64(end)

        # Use np.searchsorted to find the appropriate indices for the start and end dates
        start_idx = 0 if start is None else np.searchsorted(ts_sorted, start_np, side='left')
        end_idx = len(ts_sorted) if end is None else np.searchsorted(ts_sorted, end_np, side='right')

        # Use pa.Table's slice method to slice the table
        length = end_idx - start_idx
        return table.slice(offset=start_idx, length=length-1)