# pipeline.py
from transform.dimensions.dim_airline import transform_airline_dim
from transform.dimensions.dim_aircraft import transform_aircraft_dim
from transform.dimensions.dim_route import transform_route_dim
from transform.dimensions.dim_dep_delay import transform_dep_delay_dim
from transform.dimensions.dim_arr_delay import transform_arr_delay_dim
from transform.dimensions.dim_date import transform_date_dim
from transform.dimensions.dim_time import transform_time_dim
from transform.facts.fact_flight import transform_fact_flight

def run_transformations(raw_data):
    """
    Run all transformations to convert raw data into dimensional model.
    
    Args:
        raw_data: Dictionary containing raw data DataFrames
        
    Returns:
        Dictionary of transformed dimension and fact tables
    """
    # 1️⃣ Airline dimension
    dim_airline_df = transform_airline_dim(raw_data["airline"])
    print("1️⃣ Airline dimension complete")

    # 2️⃣ Aircraft dimension
    dim_aircraft_df = transform_aircraft_dim(raw_data["aircraft"])
    print("2️⃣ Aircraft dimension complete")

    # 3️⃣ Route dimension
    dim_route_df = transform_route_dim(raw_data["route"])
    print("3️⃣ Route dimension complete")

    # 4️⃣ Departure delay dimension
    dim_dep_delay_df = transform_dep_delay_dim(raw_data["dep_delay"])
    print("4️⃣ Departure Delay dimension complete")

    # 5️⃣ Arrival delay dimension
    dim_arr_delay_df = transform_arr_delay_dim(raw_data["arr_delay"])
    print("5️⃣ Arrival Delay dimension complete")

    # 6️⃣ Date dimension
    dim_date_df = transform_date_dim(raw_data["flight"])
    print("6️⃣ Date dimension complete")

    # 7️⃣ Time dimension
    dim_time_df = transform_time_dim(raw_data["flight"])
    print("7️⃣ Time dimension complete")

    # 8️⃣ Fact table: Flights
    fact_flight_df = transform_fact_flight(
        raw_data["flight"],
        dim_airline_df,
        dim_aircraft_df,
        dim_route_df,
        dim_dep_delay_df,
        dim_arr_delay_df,
        dim_date_df,
        dim_time_df
    )
    print("8️⃣ Flight fact table complete")

    return {
        "dim_airline": dim_airline_df,
        "dim_aircraft": dim_aircraft_df,
        "dim_route": dim_route_df,
        "dim_dep_delay": dim_dep_delay_df,
        "dim_arr_delay": dim_arr_delay_df,
        "dim_date": dim_date_df,
        "dim_time": dim_time_df,
        "fact_flight": fact_flight_df
    }