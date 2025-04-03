from datetime import datetime
from py_data.tablify import create_random_tables, create_single_table
from rs_cutter import RsCutter

def main():

    tables = create_random_tables(6, datetime(2022, 1, 1), datetime(2023, 1, 1))

    cutter = RsCutter(tables)

    new_tables = cutter.slice(datetime(2022, 3, 1), datetime(2022, 4, 1))

    print(type(new_tables.get("Table 1")))

if __name__ == "__main__":
    main()