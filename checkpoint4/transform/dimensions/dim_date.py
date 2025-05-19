from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, row_number
from pyspark.sql.window import Window

def transform_date_dim(flight_df: DataFrame) -> DataFrame:
    """
    Transform date data from flights into the date dimension table
    
    Args:
        flight_df: DataFrame containing flight data from the source with date columns
        
    Returns:
        DataFrame: Transformed date dimension table
    """
    print("Transforming date dimension...")
    
    # Select unique date combinations
    dim_date = flight_df.select(
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week")
    ).distinct()
    
    # Add surrogate key using window function to ensure deterministic ordering
    window_spec = Window.orderBy("year", "month", "day")
    dim_date = dim_date.withColumn("date_tk", row_number().over(window_spec))
    
    # Select final columns in the right order
    dim_date = dim_date.select(
        "date_tk",
        "year",
        "month",
        "day",
        "day_of_week"
    )
    
    print(f"Created date dimension with {dim_date.count()} records")
    return dim_date