from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col

def transform_arr_delay_dim(arr_delay_df: DataFrame) -> DataFrame:
    """
    Transform arrival delay data into the arrival delay dimension table
    
    Args:
        arr_delay_df: DataFrame containing arrival delay data from the source
        
    Returns:
        DataFrame: Transformed arrival delay dimension table
    """
    print("Transforming arrival delay dimension...")
    
    # Select and rename columns as needed
    dim_arr_delay = arr_delay_df.select(
        col("id").alias("source_id"),  # Keep original ID for mapping
        col("reason_arr_delay").alias("reason"),
        col("arr_delay_time").alias("delay_time")
    )
    
    # Add surrogate key
    dim_arr_delay = dim_arr_delay.withColumn("arr_delay_tk", monotonically_increasing_id() + 1)
    
    # Select final columns in the right order
    dim_arr_delay = dim_arr_delay.select(
        "arr_delay_tk",
        "reason",
        "delay_time",
        "source_id"  # Keep for joining in fact table transformation
    )
    
    print(f"Created arrival delay dimension with {dim_arr_delay.count()} records")
    return dim_arr_delay