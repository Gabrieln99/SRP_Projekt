# run_loading.py
from pyspark.sql import DataFrame

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    """
    Write a Spark DataFrame to MySQL table.
    
    Args:
        spark_df: PySpark DataFrame to write
        table_name: Target table name
        mode: Write mode (append, overwrite, ignore, etc.)
    """
    # MySQL connection properties for the star schema database
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/star_shema?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    print(f"Writing to table {table_name} with mode {mode}...")
    
    # Write the DataFrame to MySQL
    spark_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    
    print(f"Done writing to {table_name}.")