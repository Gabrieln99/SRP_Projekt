# testirano u MySQL - u radi

import unittest
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from pandas.testing import assert_frame_equal

class TestFlightDatabase(unittest.TestCase):
    def setUp(self):
    
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost/flights_db')
        self.connection = self.engine.connect()
        
        
        self.df = pd.read_csv("D:/fax_/treca_godina/skladistenje_rudarenje/flights_najbolji_PROCESSED.csv")
        
        
        query = text("""
        SELECT 
            f.year, 
            f.month, 
            f.day, 
            f.day_of_week, 
            f.dep_time, 
            f.arr_time, 
            f.hour, 
            f.minute, 
            f.air_time,
            f.flight_num,
            a.carrier,
            a.airline_name,
            ac.tailnum,
            r.origin,
            r.destination,
            r.distance,
            r.sched_dep_time,
            r.sched_arr_time,
            dd.reason_dep_delay,
            dd.dep_delay_time,
            ad.reason_arr_delay,
            ad.arr_delay_time
        FROM 
            flight f
            JOIN aircraft ac ON f.aircraft_fk = ac.id
            JOIN airline a ON ac.airline_fk = a.id
            JOIN route r ON f.route_fk = r.id
            JOIN dep_delay dd ON f.dep_delay_fk = dd.id
            JOIN arr_delay ad ON f.arr_delay_fk = ad.id
        ORDER BY 
            f.id ASC
        """)
        
        result = self.connection.execute(query)  
        self.db_df = pd.DataFrame(result.fetchall())  
        self.db_df.columns = result.keys()  
        
        
        numeric_columns = ['distance', 'air_time', 'dep_delay_time', 'arr_delay_time']
        for col in numeric_columns:
            if col in self.df.columns and col in self.db_df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                self.db_df[col] = pd.to_numeric(self.db_df[col], errors='coerce')

   
    def test_columns(self):
        
        csv_columns = self.df.columns.tolist()
        if 'id' in csv_columns:
            csv_columns.remove('id')
            
        
        csv_columns = sorted(csv_columns)
        db_columns = sorted(self.db_df.columns)
        
        print(f"CSV columns: {csv_columns}")
        print(f"DB columns: {db_columns}")
        
        self.assertSetEqual(set(csv_columns), set(db_columns), 
                           "The columns in the CSV file and database should be the same (excluding 'id')")

    
    def test_row_count(self):
        """Test that the number of rows in the database matches the CSV file"""
        
        csv_row_count = len(self.df)
        db_row_count = len(self.db_df)
        
        print(f"CSV row count: {csv_row_count}")
        print(f"DB row count: {db_row_count}")
        
        self.assertEqual(csv_row_count, db_row_count, 
                         f"Row count mismatch. CSV: {csv_row_count}, DB: {db_row_count}")
    
    
    def test_sample_data(self):
        """Test a sample of data to ensure the import was correct"""
       
        key_columns = ['year', 'month', 'day', 'carrier', 'flight_num', 'origin', 'destination']
        
       
        key_columns = [col for col in key_columns if col in self.df.columns and col in self.db_df.columns]
        
        
        csv_sample = self.df[key_columns].head(10).sort_values(by=key_columns).reset_index(drop=True)
        db_sample = self.db_df[key_columns].head(10).sort_values(by=key_columns).reset_index(drop=True)
        
        print("\nCSV Sample:")
        print(csv_sample)
        print("\nDB Sample:")
        print(db_sample)
        
        
        assert_frame_equal(
            csv_sample,
            db_sample,
            check_dtype=False  
        )
        
        print("Sample comparison successful")

    
    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()