from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, row_number
from pyspark.sql.window import Window

def transform_time_dim(flight_df: DataFrame) -> DataFrame:
    """
    Transform time data from flights into the time dimension table
    
    Args:
        flight_df: DataFrame containing flight data from the source with time columns
        
    Returns:
        DataFrame: Transformed time dimension table
    """
    print("Transforming time dimension...")
    
    # Select unique time combinations
    dim_time = flight_df.select(
        col("dep_time"),
        col("arr_time"),
        col("sched_dep_time"),
        col("sched_arr_time"),
        col("hour"),
        col("minute")
    ).distinct()
    
    # Add surrogate key using window function to ensure deterministic ordering
    window_spec = Window.orderBy("hour", "minute", "sched_dep_time")
    dim_time = dim_time.withColumn("time_tk", row_number().over(window_spec))
    
    # Select final columns in the right order
    dim_time = dim_time.select(
        "time_tk",
        "dep_time",
        "arr_time",
        "sched_dep_time",
        "sched_arr_time",
        "hour",
        "minute"
    )
    
    print(f"Created time dimension with {dim_time.count()} records")
    return dim_time