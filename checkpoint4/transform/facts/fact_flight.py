from pyspark.sql import DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id, trim

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
    """Transform flight data into fact table"""
    print("Creating fact flight table...")
    
    # Start building fact table with flight data
    fact_flight = flight_df.select(
        col("id").alias("flight_source_id"),
        col("carrier"),
        col("tailnum"),
        col("origin"),
        col("destination"),
        col("dep_delay_time").alias("dep_delay_tk"),
        col("arr_delay_time").alias("arr_delay_tk"),
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week"),
        col("dep_time"),
        col("arr_time"),
        col("sched_dep_time"),
        col("sched_arr_time"),
        col("hour"),
        col("minute"),
        col("air_time"),
        col("flight_num"),
        col("distance").alias("flight_distance")  # Rename distance to avoid ambiguity
    )
    
    # Join with dimensions to get surrogate keys
    
    # Airline dimension
    fact_flight = fact_flight.join(
        dim_airline_df,
        trim(fact_flight.carrier) == trim(dim_airline_df.carrier),
        "left"
    )
    
    # Aircraft dimension
    fact_flight = fact_flight.join(
        dim_aircraft_df,
        trim(fact_flight.tailnum) == trim(dim_aircraft_df.tailnum),
        "left"
    )
    
    # Route dimension - rename distance column to avoid ambiguity
    modified_route_df = dim_route_df.withColumnRenamed("distance", "route_distance")
    fact_flight = fact_flight.join(
        modified_route_df,
        (trim(fact_flight.origin) == trim(modified_route_df.origin)) &
        (trim(fact_flight.destination) == trim(modified_route_df.destination)),
        "left"
    )
    
    # Departure delay dimension
    fact_flight = fact_flight.join(
        dim_dep_delay_df,
        fact_flight.dep_delay_tk == dim_dep_delay_df.dep_delay_tk,
        "left"
    )
    
    # Arrival delay dimension
    fact_flight = fact_flight.join(
        dim_arr_delay_df,
        fact_flight.arr_delay_tk == dim_arr_delay_df.arr_delay_tk,
        "left"
    )
    
    # Date dimension
    fact_flight = fact_flight.join(
        dim_date_df,
        (fact_flight.year == dim_date_df.year) &
        (fact_flight.month == dim_date_df.month) &
        (fact_flight.day == dim_date_df.day) &
        (fact_flight.day_of_week == dim_date_df.day_of_week),
        "left"
    )
    
    # Time dimension
    fact_flight = fact_flight.join(
        dim_time_df,
        (fact_flight.dep_time == dim_time_df.dep_time) &
        (fact_flight.sched_dep_time == dim_time_df.sched_dep_time),
        "left"
    )
    
    # Create surrogate key for fact table
    fact_flight = fact_flight.withColumn("flight_sk", monotonically_increasing_id() + 1)
    
    # Select final columns with consistent naming
    final_fact_flight = fact_flight.select(
        "flight_sk",
        col("airline_sk"),
        col("aircraft_sk"),
        col("route_sk"),
        col("dep_delay_sk"),
        col("arr_delay_sk"),
        col("date_sk"),
        col("time_sk"),
        "air_time",
        "flight_distance",  # Use renamed column
        "flight_num"
    )
    
    print(f"Created fact flight table with {final_fact_flight.count()} records")
    return final_fact_flight