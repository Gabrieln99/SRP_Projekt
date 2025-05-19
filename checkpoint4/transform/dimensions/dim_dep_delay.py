# dim_dep_delay.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_dep_delay_dim(dep_delay_df):
    """
    Transform departure delay data into the star schema dimension format.
    
    Args:
        dep_delay_df: DataFrame containing departure delay data from the transactional model
        
    Returns:
        DataFrame with departure delay dimension structure ready for loading
    """
    print("Transforming departure delay dimension...")
    
    # Select relevant columns and rename to match star schema
    dep_delay_dim = dep_delay_df.select(
        col("reason_dep_delay").alias("reason"),
        col("dep_delay_time").alias("delay_time")
    )
    
    # Drop duplicates
    dep_delay_dim = dep_delay_dim.dropDuplicates(["reason", "delay_time"])
    
    # Generate surrogate key
    dep_delay_dim = dep_delay_dim.withColumn("dep_delay_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    dep_delay_dim = dep_delay_dim.select(
        "dep_delay_tk",
        "reason",
        "delay_time"
    )
    
    print(f"Created departure delay dimension with {dep_delay_dim.count()} records")
    return dep_delay_dim