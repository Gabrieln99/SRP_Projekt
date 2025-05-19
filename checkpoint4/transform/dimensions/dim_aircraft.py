# dim_aircraft.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_aircraft_dim(aircraft_df):
    """
    Transform aircraft data into the star schema dimension format.
    
    Args:
        aircraft_df: DataFrame containing aircraft data from the transactional model
        
    Returns:
        DataFrame with aircraft dimension structure ready for loading
    """
    print("Transforming aircraft dimension...")
    
    # Select relevant columns
    aircraft_dim = aircraft_df.select(
        col("tailnum")
    )
    
    # Drop duplicates and null values
    aircraft_dim = aircraft_dim.dropDuplicates(["tailnum"])
    aircraft_dim = aircraft_dim.dropna(subset=["tailnum"])
    
    # Generate surrogate key
    aircraft_dim = aircraft_dim.withColumn("aircraft_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    aircraft_dim = aircraft_dim.select(
        "aircraft_tk",
        "tailnum"
    )
    
    print(f"Created aircraft dimension with {aircraft_dim.count()} records")
    return aircraft_dim