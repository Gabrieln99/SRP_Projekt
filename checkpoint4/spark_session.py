from pyspark.sql import SparkSession
import os
import sys

def get_spark_session(app_name="ETL_App"):
    # Set up Hadoop environment for Windows
    os.environ['HADOOP_HOME'] = os.path.join(os.path.dirname(__file__), 'hadoop')
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin' + os.pathsep + os.environ['PATH']

    # Specify the path to the MySQL connector JAR using raw string
    connector_path = r"D:\fax_\treca_godina\skladistenje_rudarenje\checkpoint4\connectors\mysql-connector-j-9.2.0.jar"
    
    # Check if the connector exists
    if not os.path.exists(connector_path):
        raise FileNotFoundError(f"MySQL connector JAR not found at: {connector_path}")
    
    # Create and return the SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", connector_path) \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.extraClassPath", connector_path) \
        .getOrCreate()