import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL_App"):
    """
    Create and return a Spark session with MySQL connector configured.
    """
    # Set up Hadoop home for Windows to avoid warnings
    hadoop_home = os.path.join(Path(__file__).parent.absolute(), "hadoop")
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]
    
    # Get the directory where the script is located
    current_dir = Path(__file__).parent.absolute()
    
    # Define connector path relative to the script location
    connector_dir = os.path.join(current_dir, "connectors")
    connector_path = os.path.join(connector_dir, "mysql-connector-j-9.2.0.jar")
    
    # Create connectors directory if it doesn't exist
    if not os.path.exists(connector_dir):
        os.makedirs(connector_dir)
        logging.warning(f"Created connectors directory at: {connector_dir}")
    
    # Check if connector exists
    if not os.path.exists(connector_path):
        # Try alternate filenames
        alternate_names = [
            "mysql-connector-java-8.0.28.jar",
            "mysql-connector-java-8.0.30.jar",
            "mysql-connector-j-8.3.0.jar"
        ]
        
        for name in alternate_names:
            alt_path = os.path.join(connector_dir, name)
            if os.path.exists(alt_path):
                connector_path = alt_path
                break
        else:  # No connector found
            error_msg = f"MySQL connector JAR not found at: {connector_path}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)
    
    # Build the Spark session with additional configurations to suppress warnings
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", connector_path) \
        .config("spark.driver.extraClassPath", connector_path) \
        .config("spark.executor.extraClassPath", connector_path) \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.hadoop.fs.permissions.umask-mode", "022") \
        .config("spark.hadoop.dfs.permissions", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.warehouse.dir", os.path.join(current_dir, "spark-warehouse")) \
        .getOrCreate()
    
    # Set log level to ERROR to prevent warnings
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark