# extract_mysql.py
from spark_session import get_spark_session

def extract_table(table_name):
    """
    Extract a single table from MySQL database.
    
    Args:
        table_name: Name of the table to extract
        
    Returns:
        PySpark DataFrame containing the table data
    """
    # Create or get the Spark session
    spark = get_spark_session("ETL_Extract_MySQL")
    
    # MySQL connection properties
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/flights_db?useSSL=false"  # transactional model
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    print(f"Extracting table: {table_name}")
    
    # Read the table from MySQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties
    )
    
    print(f"Extracted {df.count()} rows from {table_name}")
    
    return df

def extract_all_tables():
    """
    Extract all required tables from MySQL database.
    
    Returns:
        Dictionary of table names to DataFrames
    """
    print("Starting extraction of all MySQL tables...")
    
    # Extract all required tables
    tables = {
        "airline": extract_table("airline"),
        "aircraft": extract_table("aircraft"),
        "route": extract_table("route"),
        "dep_delay": extract_table("dep_delay"),
        "arr_delay": extract_table("arr_delay"),
        "flight": extract_table("flight")
    }
    
    print("Completed extraction of all MySQL tables")
    
    return tables