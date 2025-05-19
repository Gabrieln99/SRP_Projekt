from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col

def transform_aircraft_dim(aircraft_df: DataFrame) -> DataFrame:
    """
    Transform aircraft data into the aircraft dimension table
    
    Args:
        aircraft_df: DataFrame containing aircraft data from the source
        
    Returns:
        DataFrame: Transformed aircraft dimension table
    """
    print("Transforming aircraft dimension...")
    
    # Select and rename columns as needed
    dim_aircraft = aircraft_df.select(
        col("id").alias("source_id"),  # Keep original ID for mapping
        col("tailnum"),
        col("airline_fk")  # Keep for reference but won't be in final dimension
    )
    
    # Add surrogate key
    dim_aircraft = dim_aircraft.withColumn("aircraft_tk", monotonically_increasing_id() + 1)
    
    # Select final columns in the right order
    dim_aircraft = dim_aircraft.select(
        "aircraft_tk",
        "tailnum",
        "source_id",  # Keep for joining in fact table transformation
        "airline_fk"   # Keep for reference to airline dimension
    )
    
    print(f"Created aircraft dimension with {dim_aircraft.count()} records")
    return dim_aircraft