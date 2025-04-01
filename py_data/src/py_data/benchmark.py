from datetime import datetime
import gc
from time import perf_counter
from py_data.tablify import create_random_tables, create_single_table
from py_data.utils import get_rss_memory
from rs_cutter import RsCutter
import matplotlib.pyplot as plt

def bench():
    construction_time_vs_rows()
    memory_diff_vs_rows()
    memory_size_vs_rows()

def construction_time_vs_rows():
    table_sizes = [(10, 100), (100, 100), (1000, 100), (10000, 100), (1000000, 100)]
    init_times = []
    row_counts = []

    for rows, cols in table_sizes:
        table_dict = create_single_table(rows, cols)

        start_time = perf_counter()
        cutter = RsCutter(table_dict)
        end_time = perf_counter()

        assert table_dict.get("Table 1").num_rows == cutter.total_row_count()

        print(f"TOTAL NUMBER OF ROWS: {cutter.total_row_count()}")

        row_counts.append(cutter.total_row_count())

        init_times.append(end_time - start_time)

    plt.plot(row_counts, init_times, marker='o')
    plt.xscale('log')
    plt.xlabel('Number of Rows')
    plt.ylabel('Construction Time (seconds)')
    plt.title('RsCutter Construction Time vs Table Size')
    plt.show()

def memory_diff_vs_rows():
    table_sizes = [(10, 100), (100, 100), (1000, 100), (10000, 100), (1000000, 100)]
    memory_diffs = []
    row_counts = []

    for rows, cols in table_sizes:
        gc.collect()
        
        table_dict = create_single_table(rows, cols)

        memory_before = get_rss_memory()

        cutter = RsCutter(table_dict)

        memory_after = get_rss_memory()

        assert table_dict.get("Table 1").num_rows == cutter.total_row_count()

        print(f"TOTAL NUMBER OF ROWS: {cutter.total_row_count()}")

        row_counts.append(cutter.total_row_count())

        memory_diff = memory_after - memory_before

        memory_diff_mb = memory_diff / (1024 * 1024)
        memory_diffs.append(memory_diff_mb)

    plt.plot(row_counts, memory_diffs, marker='o', color='g')
    plt.xscale('log')
    plt.xlabel('Number of Rows')
    plt.ylabel('Memory Usage Difference (MB)')
    plt.title('RSS Memory Usage Difference vs Table Size')
    plt.show()

def memory_size_vs_rows():
    table_sizes = [(10, 100), (100, 100), (1000, 100), (10000, 100), (1000000, 100)]
    memory_counts = []
    row_counts = []

    for rows, cols in table_sizes:
        gc.collect()
        
        table_dict = create_single_table(rows, cols)

        cutter = RsCutter(table_dict)

        memory = get_rss_memory()

        assert table_dict.get("Table 1").num_rows == cutter.total_row_count()

        print(f"TOTAL NUMBER OF ROWS: {cutter.total_row_count()}")

        row_counts.append(cutter.total_row_count())

        memory = memory / (1024 * 1024)
        memory_counts.append(memory)

    plt.plot(row_counts, memory_counts, marker='o', color='g')
    plt.xscale('log')
    plt.xlabel('Number of Rows')
    plt.ylabel('Memory Usage Difference (MB)')
    plt.title('RSS Memory Usage Difference vs Table Size')
    plt.show()


if __name__ == "__main__":
    bench()
