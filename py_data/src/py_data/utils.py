import psutil

def get_rss_memory():
    # Get the current process memory usage (RSS)
    process = psutil.Process()    
    return process.memory_info().rss