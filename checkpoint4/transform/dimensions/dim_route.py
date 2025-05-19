# dim_route.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_route_dim(route_df):
    """
    Transform route data into the star schema dimension format.
    
    Args:
        route_df: DataFrame containing route data from the transactional model
        
    Returns:
        DataFrame with route dimension structure ready for loading
    """
    print("Transforming route dimension...")
    
    # Select relevant columns
    route_dim = route_df.select(
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
    
    # Drop duplicates based on origin and destination
    route_dim = route_dim.dropDuplicates(["origin", "destination"])
    
    # Generate surrogate key
    route_dim = route_dim.withColumn("route_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    route_dim = route_dim.select(
        "route_tk",
        "origin",
        "destination",
        "departure_city",
        "departure_country",
        "departure_airport_name",
        "destination_city",
        "destination_country",
        "destination_airport_name",
        "distance"
    )
    
    print(f"Created route dimension with {route_dim.count()} records")
    return route_dim