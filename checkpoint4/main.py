# main.py
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql
import os
import logging
from pathlib import Path
from pyspark.sql.functions import lit, col

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Unset SPARK_HOME if it exists to prevent Spark session conflicts
os.environ.pop("SPARK_HOME", None)

def main():
    spark = None
    try:
        spark = get_spark_session("Flight_ETL_Process")
        
        # Clear any cached data
        spark.catalog.clearCache()
        
        # =================== EXTRACT PHASE ===================
        logger.info("======= STARTING EXTRACT PHASE =======")
        
        # 1. Extract data from MySQL (80% of data)
        logger.info("Extracting data from MySQL database (80% of data)")
        mysql_data = extract_all_tables()
        
        # 2. Extract data from CSV (20% of data)
        logger.info("Extracting data from CSV file (20% of data)")
        csv_path = Path("D:/fax_/treca_godina/skladistenje_rudarenje/SRP_Projekt/checkpoint2/flights_najbolji_PROCESSED_20.csv")

        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        csv_flight_data = extract_from_csv(str(csv_path))
        
        # Add source system indicator to track data origin
        for table_name, df in mysql_data.items():
            mysql_data[table_name] = df.withColumn("source_system", lit("MYSQL"))
        
        csv_flight_data = csv_flight_data.withColumn("source_system", lit("CSV"))
        
        # =================== TRANSFORM PHASE ===================
        logger.info("======= STARTING TRANSFORM PHASE =======")
        
        # 1. Process MySQL data first (80%)
        logger.info("Transforming MySQL data (80%)")
        transformed_mysql = run_transformations(mysql_data)
        
        # 2. Process combined data (MySQL 80% + CSV 20%)
        logger.info("Processing combined data (MySQL 80% + CSV 20%)")
        
        # Create a combined dataset with MySQL data
        combined_data = mysql_data.copy()
        
        # Add CSV flight data (20%) to the combined dataset
        # We need to ensure the CSV data has all the columns needed for the transformation
        required_columns = set(mysql_data["flight"].columns)
        existing_columns = set(csv_flight_data.columns)
        
        # Create expressions for selecting columns from CSV data
        # For missing columns, we'll use null values
        select_expressions = []
        for column in required_columns:
            if column in existing_columns:
                select_expressions.append(col(column))
            else:
                select_expressions.append(lit(None).alias(column))
        
        # Select columns from CSV data to match MySQL schema
        csv_flight_data_matched = csv_flight_data.select(*select_expressions)
        
        # Combine MySQL and CSV flight data
        combined_data["flight"] = mysql_data["flight"].unionByName(
            csv_flight_data_matched, 
            allowMissingColumns=True
        )
        
        # Transform the combined data
        logger.info("Transforming combined data")
        transformed_combined = run_transformations(combined_data)
        
        # =================== LOAD PHASE ===================
        logger.info("======= STARTING LOAD PHASE =======")
        
        # Load the transformed data into the star schema
        for table_name, df in transformed_combined.items():
            logger.info(f"Loading table '{table_name}' with {df.count()} records")
            write_spark_df_to_mysql(df, table_name)
        
        logger.info("ETL process completed successfully")
        
        # Close Spark session
        if spark:
            spark.stop()
        
    except FileNotFoundError as e:
        logger.error(f"{e}")  # Changed from logging.error to logger.error
        logger.error("Continuing without Spark session...")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        if spark:
            spark.stop()
        raise

if __name__ == "__main__":
    main()