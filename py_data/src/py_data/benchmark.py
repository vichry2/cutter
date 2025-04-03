from datetime import datetime
from functools import lru_cache
import gc
from time import perf_counter
from py_data.cutter import Cutter
from py_data.tablify import create_multiple_tables, create_single_table
from py_data.utils import get_rss_memory
from rs_cutter import RsCutter
import matplotlib.pyplot as plt

def profile():
    memory_diff_vs_rows()
    memory_size_vs_rows()

def bench():
    slicer_time_vs_rows()
    slice_time_vs_number_of_tables()
    construction_time_vs_rows()
    construction_time_with_lru_cache()

# Takes 30+ seconds to run
def slice_time_vs_number_of_tables():
    number_of_tables = [10, 50, 100, 250, 500, 1000, 1500, 2000]
    fixed_rows = 150_000
    fixed_columns = 10
    
    rs_times = []
    rs_p_times = []
    py_times = []

    # Loop through different numbers of tables
    for num_tables in number_of_tables:
        # Create multiple tables with the given number of tables, rows, and columns
        tables = create_multiple_tables(num_tables, fixed_rows, fixed_columns, datetime(2022, 1, 1))
        
        # Time for Rust Slicer
        cutter_rs = RsCutter(tables)
        start_rs = perf_counter()
        tbls_rs = cutter_rs.slice(datetime(2022, 1, 1, 0, 4), datetime(2022, 1, 1, 0, 30))
        end_rs = perf_counter()
        
         # Time for Rust Slicer w/ Parralel
        cutter_rs = RsCutter(tables)
        start_rs_p = perf_counter()
        tbls_rs = cutter_rs.slice(datetime(2022, 1, 1, 0, 4), datetime(2022, 1, 1, 0, 30), parralel=True)
        end_rs_p = perf_counter()

        # Time for Python Slicer
        cutter_py = Cutter(tables)
        start_py = perf_counter()
        tbls_py = cutter_py.slice(datetime(2022, 1, 1, 0, 4), datetime(2022, 1, 1, 0, 30))
        end_py = perf_counter()

        try:
            assert tbls_py == tbls_rs
        except AssertionError:
            print(f"Assertion failed for {num_tables} tables!")

        # Append the times for both slicers
        rs_times.append(end_rs - start_rs)
        rs_p_times.append(end_rs_p - start_rs_p)
        py_times.append(end_py - start_py)

    # Plotting the slice times vs the number of tables
    plt.figure(figsize=(10, 6))
    plt.plot(number_of_tables, rs_times, label="Rust Cutter", marker='o', color='b')
    plt.plot(number_of_tables, py_times, label="Python Cutter", marker='o', color='r')
    plt.plot(number_of_tables, rs_p_times, label="Rust Cutter Parralel", marker='o', color='y')
    
    # Adding labels and title
    plt.xlabel('Number of Tables')
    plt.ylabel('Slicing Time (seconds)')
    plt.title('Slicing Time vs Number of Tables (Rust vs Python)')
    plt.legend()

    # Show the plot
    plt.show()

    # Plotting the slice times vs the number of tables
    plt.figure(figsize=(10, 6))
    plt.plot(number_of_tables, rs_times, label="Rust Cutter", marker='o', color='b')
    plt.plot(number_of_tables, rs_p_times, label="Rust Cutter Parralel", marker='o', color='y')
    
    # Adding labels and title
    plt.xlabel('Number of Tables')
    plt.ylabel('Slicing Time (seconds)')
    plt.title('Slicing Time vs Number of Tables (Rust -- Parralel vs Non-parralel)')
    plt.legend()

    # Show the plot
    plt.show()




def construction_time_vs_rows():
    table_sizes = [(10, 100), (100, 100), (1000, 100), (10000, 100), (1000000, 100)]
    init_times_rs = []
    init_times_py = []
    row_counts = []

    for rows, cols in table_sizes:
        table_dict = create_single_table(rows, cols)

        # Measure time for Rust Cutter
        start_time_rs = perf_counter()
        cutter_rs = RsCutter(table_dict)
        end_time_rs = perf_counter()

        # Measure time for Python Cutter
        start_time_py = perf_counter()
        cutter_py = Cutter(table_dict)
        end_time_py = perf_counter()

        # Store results
        row_counts.append(cutter_rs.total_row_count())
        init_times_rs.append(end_time_rs - start_time_rs)
        init_times_py.append(end_time_py - start_time_py)

    # Plotting the construction times
    plt.figure(figsize=(10, 6))
    plt.plot(row_counts, init_times_rs, label="Rust Cutter", marker='o', color='b')
    plt.plot(row_counts, init_times_py, label="Python Cutter", marker='o', color='r')

    # Adding labels and title
    plt.xscale('log')  # Log scale for the x-axis to better visualize large differences
    plt.xlabel('Number of Rows')
    plt.ylabel('Construction Time (seconds)')
    plt.title('Cutter Construction Time vs Table Size (Rust vs Python)')
    plt.legend()

    # Show the plot
    plt.show()

def slicer_time_vs_rows():
    table_sizes = [(10, 100), (100, 100), (1000, 100), (10000, 100), (1000000, 100)]
    slicer_times_rs = []
    slicer_times_py = []
    row_counts = []

    for rows, cols in table_sizes: 
        table_dict = create_single_table(rows, cols, datetime(2022, 1, 1))

        cutter = RsCutter(table_dict)
        start_rs = perf_counter()
        cutter.slice(datetime(2022, 1, 1, 4), datetime(2022, 1, 1, 0, 30))
        end_rs = perf_counter()
        
        row_counts.append(cutter.total_row_count())

        cutter = Cutter(table_dict)
        start_py = perf_counter()
        cutter.slice(datetime(2022, 1, 1, 4, 0, 0), datetime(2022, 1, 1, 0, 30, 0))
        end_py = perf_counter()


        slicer_times_rs.append(end_rs - start_rs)
        slicer_times_py.append(end_py - start_py)

    plt.figure(figsize=(10, 6))
    plt.plot(row_counts, slicer_times_rs, label="Rust Slicer", marker='o', color='b')
    plt.plot(row_counts, slicer_times_py, label="Python Slicer", marker='o', color='r')
    plt.xscale('log')      
    # Adding labels and title
    plt.xlabel('Number of Rows')
    plt.ylabel('Time (seconds)')
    plt.title('Slicer Time vs. Number of Rows')
    plt.legend()

    # Show the plot
    plt.show()

def construction_time_with_lru_cache():
    
    table = create_single_table(300, 100)

    my_dict = {
        "tables 1" : table
    }
    
    @lru_cache()
    def create_slicer_rs(key):
        return RsCutter(my_dict.get(key))
    
    @lru_cache()
    def create_slicer_py(key):
        return Cutter(my_dict.get(key))
    
    rs_init = []
    py_init = []

    iters = [1,2,3,4,5]

    for _ in iters:
        start_rs = perf_counter()
        create_slicer_rs("tables 1")
        end_rs = perf_counter()

        start_py = perf_counter()
        create_slicer_py("tables 1")
        end_py = perf_counter()

        rs_init.append(end_rs - start_rs)
        py_init.append(end_py - end_py)

    plt.figure(figsize=(10, 6))
    plt.plot(iters, rs_init, label="Rust Cutter with LRU Cache", marker='o', color='b')
    plt.plot(iters, py_init, label="Python Cutter with LRU Cache", marker='o', color='r')

    plt.xlabel('Iteration')
    plt.ylabel('Construction Time (seconds)')
    plt.title('Cutter Construction Time with LRU Cache (Rust vs Python)')
    plt.legend()

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

        row_counts.append(cutter.total_row_count())

        memory = memory / (1024 * 1024)
        memory_counts.append(memory)

    plt.plot(row_counts, memory_counts, marker='o', color='g')
    plt.xscale('log')
    plt.xlabel('Number of Rows')
    plt.ylabel('Memory Usage (MB)')
    plt.title('RSS Memory Usage vs Table Size')
    plt.show()


if __name__ == "__main__":
    bench()
    profile()
