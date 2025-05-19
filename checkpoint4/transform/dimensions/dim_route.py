from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col

def transform_route_dim(route_df: DataFrame) -> DataFrame:
    """
    Transform route data into the route dimension table
    
    Args:
        route_df: DataFrame containing route data from the source
        
    Returns:
        DataFrame: Transformed route dimension table
    """
    print("Transforming route dimension...")
    
    # Select and rename columns as needed
    dim_route = route_df.select(
        col("id").alias("source_id"),  # Keep original ID for mapping
        col("origin"),
        col("destination"),
        col("departure_city"),
        col("departure_country"),
        col("departure_airport_name"),
        col("destination_city"),
        col("destination_country"),
        col("destination_airport_name"),
        col("distance")
    )
    
    # Add surrogate key
    dim_route = dim_route.withColumn("route_tk", monotonically_increasing_id() + 1)
    
    # Select final columns in the right order
    dim_route = dim_route.select(
        "route_tk",
        "origin",
        "destination",
        "departure_city",
        "departure_country",
        "departure_airport_name",
        "destination_city",
        "destination_country",
        "destination_airport_name",
        "distance",
        "source_id"  # Keep for joining in fact table transformation
    )
    
    print(f"Created route dimension with {dim_route.count()} records")
    return dim_route