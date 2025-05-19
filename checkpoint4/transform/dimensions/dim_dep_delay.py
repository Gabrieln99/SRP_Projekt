from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col

def transform_dep_delay_dim(dep_delay_df: DataFrame) -> DataFrame:
    """
    Transform departure delay data into the departure delay dimension table
    
    Args:
        dep_delay_df: DataFrame containing departure delay data from the source
        
    Returns:
        DataFrame: Transformed departure delay dimension table
    """
    print("Transforming departure delay dimension...")
    
    # Select and rename columns as needed
    dim_dep_delay = dep_delay_df.select(
        col("id").alias("source_id"),  # Keep original ID for mapping
        col("reason_dep_delay").alias("reason"),
        col("dep_delay_time").alias("delay_time")
    )
    
    # Add surrogate key
    dim_dep_delay = dim_dep_delay.withColumn("dep_delay_tk", monotonically_increasing_id() + 1)
    
    # Select final columns in the right order
    dim_dep_delay = dim_dep_delay.select(
        "dep_delay_tk",
        "reason",
        "delay_time",
        "source_id"  # Keep for joining in fact table transformation
    )
    
    print(f"Created departure delay dimension with {dim_dep_delay.count()} records")
    return dim_dep_delay