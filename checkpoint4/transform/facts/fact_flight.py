# fact_flight.py
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql import DataFrame

def transform_fact_flight(
    flight_df: DataFrame,
    dim_airline_df: DataFrame,
    dim_aircraft_df: DataFrame,
    dim_route_df: DataFrame,
    dim_dep_delay_df: DataFrame,
    dim_arr_delay_df: DataFrame,
    dim_date_df: DataFrame,
    dim_time_df: DataFrame
) -> DataFrame:
    """
    Transform flight data into the fact table by linking to all dimensions.
    
    Args:
        flight_df: DataFrame containing flight data
        dim_airline_df: Airline dimension DataFrame
        dim_aircraft_df: Aircraft dimension DataFrame
        dim_route_df: Route dimension DataFrame
        dim_dep_delay_df: Departure delay dimension DataFrame
        dim_arr_delay_df: Arrival delay dimension DataFrame
        dim_date_df: Date dimension DataFrame
        dim_time_df: Time dimension DataFrame
        
    Returns:
        DataFrame with fact flight structure ready for loading
    """
    print("Transforming flight fact table...")
    
    # ==== Get airline dimension keys ====
    # First, join flight_df with aircraft to get carrier information
    flight_with_airline = flight_df.join(
        dim_airline_df.select("airline_tk", "carrier"),
        flight_df["carrier"] == dim_airline_df["carrier"],
        "left"
    ).withColumnRenamed("airline_tk", "airline_id")
    
    # ==== Get aircraft dimension keys ====
    flight_with_aircraft = flight_with_airline.join(
        dim_aircraft_df.select("aircraft_tk", "tailnum"),
        flight_with_airline["tailnum"] == dim_aircraft_df["tailnum"],
        "left"
    ).withColumnRenamed("aircraft_tk", "aircraft_id")
    
    # ==== Get route dimension keys ====
    flight_with_route = flight_with_aircraft.join(
        dim_route_df.select("route_tk", "origin", "destination"),
        (flight_with_aircraft["origin"] == dim_route_df["origin"]) & 
        (flight_with_aircraft["destination"] == dim_route_df["destination"]),
        "left"
    ).withColumnRenamed("route_tk", "route_id")
    
    # ==== Get departure delay dimension keys ====
    flight_with_dep_delay = flight_with_route.join(
        dim_dep_delay_df.select(
            "dep_delay_tk", 
            col("reason").alias("dep_reason"), 
            col("delay_time").alias("dep_delay_time")
        ),
        (flight_with_route["reason_dep_delay"] == col("dep_reason")) & 
        (flight_with_route["dep_delay_time"] == col("dep_delay_time")),
        "left"
    ).withColumnRenamed("dep_delay_tk", "dep_delay_id")
    
    # ==== Get arrival delay dimension keys ====
    flight_with_arr_delay = flight_with_dep_delay.join(
        dim_arr_delay_df.select(
            "arr_delay_tk", 
            col("reason").alias("arr_reason"), 
            col("delay_time").alias("arr_delay_time")
        ),
        (flight_with_dep_delay["reason_arr_delay"] == col("arr_reason")) & 
        (flight_with_dep_delay["arr_delay_time"] == col("arr_delay_time")),
        "left"
    ).withColumnRenamed("arr_delay_tk", "arr_delay_id")
    
    # ==== Get date dimension keys ====
    flight_with_date = flight_with_arr_delay.join(
        dim_date_df.select("date_tk", "year", "month", "day"),
        (flight_with_arr_delay["year"] == dim_date_df["year"]) & 
        (flight_with_arr_delay["month"] == dim_date_df["month"]) & 
        (flight_with_arr_delay["day"] == dim_date_df["day"]),
        "left"
    ).withColumnRenamed("date_tk", "date_id")
    
    # ==== Get time dimension keys ====
    flight_with_time = flight_with_date.join(
        dim_time_df.select(
            "time_tk", "dep_time", "arr_time", 
            "sched_dep_time", "sched_arr_time", 
            "hour", "minute"
        ),
        (flight_with_date["dep_time"] == dim_time_df["dep_time"]) & 
        (flight_with_date["arr_time"] == dim_time_df["arr_time"]) & 
        (flight_with_date["sched_dep_time"] == dim_time_df["sched_dep_time"]) & 
        (flight_with_date["sched_arr_time"] == dim_time_df["sched_arr_time"]) & 
        (flight_with_date["hour"] == dim_time_df["hour"]) & 
        (flight_with_date["minute"] == dim_time_df["minute"]),
        "left"
    ).withColumnRenamed("time_tk", "time_id")
    
    # Build the final fact table with selected columns
    fact_flight = flight_with_time.select(
        monotonically_increasing_id().alias("flight_tk"),
        "airline_id",
        "aircraft_id",
        "route_id",
        "dep_delay_id",
        "arr_delay_id",
        "date_id",
        "time_id",
        "air_time",
        "flight_num"
    )
    
    # Count valid fact records (those with all dimension keys)
    valid_records = fact_flight.filter(
        col("airline_id").isNotNull() &
        col("route_id").isNotNull() &
        col("date_id").isNotNull() &
        col("time_id").isNotNull()
    ).count()
    
    print(f"Created flight fact table with {fact_flight.count()} total records")
    print(f"  - {valid_records} valid records with all required dimension keys")
    
    return fact_flight