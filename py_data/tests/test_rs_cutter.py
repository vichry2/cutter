from py_data.tablify import create_single_table
from rs_cutter import RsCutter

def test_rs_cutter():
    table = create_single_table(100, 5)

    cutter = RsCutter(table)

    assert cutter.total_row_count() == 100