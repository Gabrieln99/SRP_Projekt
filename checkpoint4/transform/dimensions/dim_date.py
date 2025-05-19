# dim_date.py
from pyspark.sql.functions import col, monotonically_increasing_id

def transform_date_dim(flight_df):
    """
    Transform date information from flight data into a date dimension table.
    
    Args:
        flight_df: DataFrame containing flight data with date columns
        
    Returns:
        DataFrame with date dimension structure ready for loading
    """
    print("Transforming date dimension...")
    
    # Select relevant date columns from flight data
    date_dim = flight_df.select(
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week")
    )
    
    # Drop duplicates to get unique date combinations
    date_dim = date_dim.dropDuplicates(["year", "month", "day"])
    
    # Generate surrogate key
    date_dim = date_dim.withColumn("date_tk", monotonically_increasing_id() + 1)
    
    # Reorder columns to match the dimension table structure
    date_dim = date_dim.select(
        "date_tk",
        "year",
        "month",
        "day",
        "day_of_week"
    )
    
    print(f"Created date dimension with {date_dim.count()} records")
    return date_dim