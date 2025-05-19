# fact_flight.py
from pyspark.sql.functions import col, monotonically_increasing_id, broadcast
from pyspark.sql import DataFrame

def transform_fact_flight(
    flight_df: DataFrame,
    dim_airline_df: DataFrame,
    dim_aircraft_df: DataFrame,
    dim_route_df: DataFrame,
    dim_dep_delay_df: DataFrame,
    dim_arr_delay_df: DataFrame,
    dim_date_df: DataFrame,
    dim_time_df: DataFrame,
    source_tables=None  # Add source_tables parameter to access original tables
) -> DataFrame:
    """
    Transform flight data into the fact table by linking to all dimensions.
    """
    print("Transforming flight fact table...")
    
    # Debug schema informacije
    print("Aircraft dim schema:")
    dim_aircraft_df.printSchema()
    
    print("Airline dim schema:")
    dim_airline_df.printSchema()
    
    print("Flight schema:")
    flight_df.printSchema()
    
    # ==== Spajanje s dim_aircraft preko aircraft_fk ====
    flight_with_aircraft = flight_df.join(
        dim_aircraft_df.select("aircraft_tk", "tailnum"),
        flight_df["aircraft_fk"] == dim_aircraft_df["aircraft_tk"],
        "left"
    ).withColumnRenamed("aircraft_tk", "aircraft_id")
    
    # ==== IMPROVED AIRLINE CONNECTION ====
    # First try direct carrier join if carrier column exists
    if "carrier" in flight_df.columns:
        flight_with_airline = flight_with_aircraft.join(
            broadcast(dim_airline_df.select("airline_tk", "carrier")),
            flight_with_aircraft["carrier"] == dim_airline_df["carrier"],
            "left"
        ).withColumnRenamed("airline_tk", "airline_id")
    else:
        # If we have access to the original aircraft table with airline_fk
        if source_tables is not None and "aircraft" in source_tables:
            print("Using source aircraft table to get airline relationship")
            # Join with the original aircraft table to get airline_fk
            flight_with_orig_aircraft = flight_df.join(
                broadcast(source_tables["aircraft"].select("id", "airline_fk")),
                flight_df["aircraft_fk"] == source_tables["aircraft"]["id"],
                "left"
            )
            
            # Then join with airline dimension using airline_fk
            flight_with_airline = flight_with_aircraft.join(
                broadcast(flight_with_orig_aircraft.select("id", "airline_fk")),
                flight_with_aircraft["aircraft_fk"] == flight_with_orig_aircraft["id"],
                "left"
            ).join(
                broadcast(dim_airline_df.select("airline_tk", "id")), 
                col("airline_fk") == dim_airline_df["id"],
                "left"
            ).withColumnRenamed("airline_tk", "airline_id")
        else:
            print("UPOZORENJE: Ni 'carrier' ni original aircraft nisu dostupni! Koristim defaultnu logiku.")
            # Try airline_fk in flight table as fallback
            if "airline_fk" in flight_df.columns:
                flight_with_airline = flight_with_aircraft.join(
                    broadcast(dim_airline_df.select("airline_tk")),
                    flight_with_aircraft["airline_fk"] == dim_airline_df["airline_tk"],
                    "left"
                ).withColumnRenamed("airline_tk", "airline_id")
            else:
                print("UPOZORENJE: Ni 'carrier' ni 'airline_fk' stupci nisu pronađeni!")
                flight_with_airline = flight_with_aircraft.withColumn("airline_id", col("aircraft_id"))
    
    # ==== Rest of the function remains the same ====
    # Spajanje s dim_route
    if "route_fk" in flight_df.columns:
        flight_with_route = flight_with_airline.join(
            broadcast(dim_route_df.select("route_tk")),
            flight_with_airline["route_fk"] == dim_route_df["route_tk"],
            "left"
        ).withColumnRenamed("route_tk", "route_id")
    else:
        if "origin" in flight_df.columns and "destination" in flight_df.columns:
            flight_with_route = flight_with_airline.join(
                broadcast(dim_route_df.select("route_tk", "origin", "destination")),
                (flight_with_airline["origin"] == dim_route_df["origin"]) & 
                (flight_with_airline["destination"] == dim_route_df["destination"]),
                "left"
            ).withColumnRenamed("route_tk", "route_id")
        else:
            print("UPOZORENJE: Ni 'route_fk' ni 'origin'/'destination' stupci nisu pronađeni!")
            flight_with_route = flight_with_airline.withColumn("route_id", col("aircraft_id"))
    
    # ==== Delay dimensions - same as before ====
    if "dep_delay_fk" in flight_df.columns:
        flight_with_dep_delay = flight_with_route.join(
            broadcast(dim_dep_delay_df.select("dep_delay_tk")),
            flight_with_route["dep_delay_fk"] == dim_dep_delay_df["dep_delay_tk"],
            "left"
        ).withColumnRenamed("dep_delay_tk", "dep_delay_id")
    else:
        # Check for columns
        if "reason_dep_delay" in flight_df.columns and "dep_delay_time" in flight_df.columns:
            flight_with_dep_delay = flight_with_route.join(
                broadcast(dim_dep_delay_df.select("dep_delay_tk", 
                                          col("reason").alias("dep_reason"), 
                                          col("delay_time").alias("dep_delay_time"))),
                (flight_with_route["reason_dep_delay"] == col("dep_reason")) & 
                (flight_with_route["dep_delay_time"] == col("dep_delay_time")),
                "left"
            ).withColumnRenamed("dep_delay_tk", "dep_delay_id")
        else:
            print("UPOZORENJE: Ni 'dep_delay_fk' ni 'reason_dep_delay'/'dep_delay_time' stupci nisu pronađeni!")
            flight_with_dep_delay = flight_with_route.withColumn("dep_delay_id", col("aircraft_id"))
    
    # ==== Arrival delay - same as before ====
    if "arr_delay_fk" in flight_df.columns:
        flight_with_arr_delay = flight_with_dep_delay.join(
            broadcast(dim_arr_delay_df.select("arr_delay_tk")),
            flight_with_dep_delay["arr_delay_fk"] == dim_arr_delay_df["arr_delay_tk"],
            "left"
        ).withColumnRenamed("arr_delay_tk", "arr_delay_id")
    else:
        if "reason_arr_delay" in flight_df.columns and "arr_delay_time" in flight_df.columns:
            flight_with_arr_delay = flight_with_dep_delay.join(
                broadcast(dim_arr_delay_df.select("arr_delay_tk", 
                                          col("reason").alias("arr_reason"), 
                                          col("delay_time").alias("arr_delay_time"))),
                (flight_with_dep_delay["reason_arr_delay"] == col("arr_reason")) & 
                (flight_with_dep_delay["arr_delay_time"] == col("arr_delay_time")),
                "left"
            ).withColumnRenamed("arr_delay_tk", "arr_delay_id")
        else:
            print("UPOZORENJE: Ni 'arr_delay_fk' ni 'reason_arr_delay'/'arr_delay_time' stupci nisu pronađeni!")
            flight_with_arr_delay = flight_with_dep_delay.withColumn("arr_delay_id", col("aircraft_id"))
    
    # ==== Date and time joins - same as before ====
    flight_with_date = flight_with_arr_delay.join(
        broadcast(dim_date_df.select("date_tk", "year", "month", "day")),
        (flight_with_arr_delay["year"] == dim_date_df["year"]) & 
        (flight_with_arr_delay["month"] == dim_date_df["month"]) & 
        (flight_with_arr_delay["day"] == dim_date_df["day"]),
        "left"
    ).withColumnRenamed("date_tk", "date_id")
    
    # ==== Time join ====
    flight_with_time = flight_with_date.join(
        dim_time_df.select("time_tk", "dep_time", "arr_time", 
                          "sched_dep_time", "sched_arr_time", 
                          "hour", "minute"),
        (flight_with_date["dep_time"] == dim_time_df["dep_time"]) & 
        (flight_with_date["arr_time"] == dim_time_df["arr_time"]) & 
        (flight_with_date["sched_dep_time"] == dim_time_df["sched_dep_time"]) & 
        (flight_with_date["sched_arr_time"] == dim_time_df["sched_arr_time"]) & 
        (flight_with_date["hour"] == dim_time_df["hour"]) & 
        (flight_with_date["minute"] == dim_time_df["minute"]),
        "left"
    ).withColumnRenamed("time_tk", "time_id")
    
    # ==== Build final fact table ====
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
    
    # Calculate valid records
    valid_records = fact_flight.filter(
        col("airline_id").isNotNull() &
        col("route_id").isNotNull() &
        col("date_id").isNotNull() &
        col("time_id").isNotNull()
    ).count()
    
    print(f"Kreirana tablica činjenica s {fact_flight.count()} ukupno zapisa")
    print(f"  - {valid_records} validnih zapisa sa svim potrebnim dimenzijskim ključevima")
    
    return fact_flight