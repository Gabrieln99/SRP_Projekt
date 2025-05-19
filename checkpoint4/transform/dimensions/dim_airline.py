# dim_airline.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_airline_dim(airline_df):
    """
    Transform airline data into the star schema dimension format.
    
    Args:
        airline_df: DataFrame containing airline data from the transactional model
        
    Returns:
        DataFrame with airline dimension structure ready for loading
    """
    print("Transforming airline dimension...")
    
    # Select relevant columns
    airline_dim = airline_df.select(
        col("carrier"),
        col("airline_name")
    )
    
    # Drop duplicates to ensure dimension integrity
    airline_dim = airline_dim.dropDuplicates(["carrier"])
    
    # Generate surrogate key
    airline_dim = airline_dim.withColumn("airline_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    airline_dim = airline_dim.select(
        "airline_tk",
        "carrier",
        "airline_name"
    )
    
    print(f"Created airline dimension with {airline_dim.count()} records")
    return airline_dim