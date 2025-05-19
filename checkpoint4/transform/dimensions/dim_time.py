# dim_time.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_time_dim(flight_df):
    """
    Transform time information from flight data into a time dimension table.
    
    Args:
        flight_df: DataFrame containing flight data with time columns
        
    Returns:
        DataFrame with time dimension structure ready for loading
    """
    print("Transforming time dimension...")
    
    # Select relevant time columns from flight data
    time_dim = flight_df.select(
        col("dep_time"),
        col("arr_time"),
        col("sched_dep_time"),
        col("sched_arr_time"),
        col("hour"),
        col("minute")
    )
    
    # Drop duplicates to get unique time combinations
    time_dim = time_dim.dropDuplicates(["dep_time", "arr_time", "sched_dep_time", "sched_arr_time", "hour", "minute"])
    
    # Generate surrogate key
    time_dim = time_dim.withColumn("time_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    time_dim = time_dim.select(
        "time_tk",
        "dep_time",
        "arr_time",
        "sched_dep_time",
        "sched_arr_time",
        "hour",
        "minute"
    )
    
    print(f"Created time dimension with {time_dim.count()} records")
    return time_dim