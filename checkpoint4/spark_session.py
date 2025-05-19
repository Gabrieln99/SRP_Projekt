import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL_App"):
    """
    Create and return a Spark session with MySQL connector configured.
    """
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
    
    # Build the Spark session with the connector properly configured
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", connector_path) \
        .config("spark.driver.extraClassPath", connector_path) \
        .config("spark.executor.extraClassPath", connector_path) \
        .getOrCreate()