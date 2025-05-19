from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, lit

def transform_airline_dim(airline_df: DataFrame) -> DataFrame:
    """
    Transform airline data into the airline dimension table
    
    Args:
        airline_df: DataFrame containing airline data from the source
        
    Returns:
        DataFrame: Transformed airline dimension table
    """
    print("Transforming airline dimension...")
    
    # Select and rename columns as needed
    dim_airline = airline_df.select(
        col("id").alias("source_id"),  # Keep original ID for mapping
        col("carrier"),
        col("airline_name")
    )
    
    # Add surrogate key
    dim_airline = dim_airline.withColumn("airline_tk", monotonically_increasing_id() + 1)
    
    # Select final columns in the right order
    dim_airline = dim_airline.select(
        "airline_tk",
        "carrier",
        "airline_name",
        "source_id"  # Keep for joining in fact table transformation
    )
    
    print(f"Created airline dimension with {dim_airline.count()} records")
    return dim_airline