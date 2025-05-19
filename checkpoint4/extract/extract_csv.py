# extract_csv.py
from spark_session import get_spark_session

def extract_from_csv(file_path):
    """
    Extract data from a CSV file using PySpark.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        PySpark DataFrame containing the CSV data
    """
    # Create or get the Spark session
    spark = get_spark_session("ETL_Extract_CSV")
    
    print(f"Extracting data from CSV: {file_path}")
    
    # Read CSV with header and schema inference
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    print(f"Extracted {df.count()} rows from CSV")
    
    return df