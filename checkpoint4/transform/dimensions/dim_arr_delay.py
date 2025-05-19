# dim_arr_delay.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_arr_delay_dim(arr_delay_df):
    """
    Transform arrival delay data into the star schema dimension format.
    
    Args:
        arr_delay_df: DataFrame containing arrival delay data from the transactional model
        
    Returns:
        DataFrame with arrival delay dimension structure ready for loading
    """
    print("Transforming arrival delay dimension...")
    
    # Select relevant columns and rename to match star schema
    arr_delay_dim = arr_delay_df.select(
        col("reason_arr_delay").alias("reason"),
        col("arr_delay_time").alias("delay_time")
    )
    
    # Drop duplicates
    arr_delay_dim = arr_delay_dim.dropDuplicates(["reason", "delay_time"])
    
    # Generate surrogate key
    arr_delay_dim = arr_delay_dim.withColumn("arr_delay_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    arr_delay_dim = arr_delay_dim.select(
        "arr_delay_tk",
        "reason",
        "delay_time"
    )
    
    print(f"Created arrival delay dimension with {arr_delay_dim.count()} records")
    return arr_delay_dim