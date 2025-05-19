from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql
import os
import logging
from pathlib import Path

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
    try:
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        spark.catalog.clearCache()
        
        # Load data
        logger.info(" Starting data extraction")
        mysql_df = extract_all_tables()
        
        # Use Path for better cross-platform compatibility
        csv_path = Path("D:/fax_/treca_godina/skladistenje_rudarenje/checkpoint2/flights_najbolji_PROCESSED_20.csv")
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        csv_df = {
            "flight": extract_from_csv(str(csv_path))  # Changed key to match expected input in transformation
        }
        
        # Check if required keys exist in the extracted data
        required_keys = ["airline", "aircraft", "route", "dep_delay", "arr_delay", "flight"]
        merged_df = {**mysql_df, **csv_df}
        
        missing_keys = [key for key in required_keys if key not in merged_df]
        if missing_keys:
            logger.error(f"Missing required data keys: {missing_keys}")
            raise KeyError(f"Missing required data keys: {missing_keys}")
            
        logger.info(" Data extraction completed")
        
        # Transform data
        logger.info(" Starting data transformation")
        load_ready_dict = run_transformations(merged_df)
        logger.info(" Data transformation completed")
        
        # Load data
        logger.info(" Starting data loading")
        for table_name, df in load_ready_dict.items():
            write_spark_df_to_mysql(df, table_name)
        logger.info(" Data loading completed")
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()