import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

import pandas as pd
from FlightRadar24 import FlightRadar24API
from functions import (distance_between_airports,
                       continent_of_airport,
                       value_or_none,
                       get_current_time)



# Extractting: from source
fr_api = FlightRadar24API()
flight_tracker = fr_api.get_flight_tracker_config()
flight_tracker.limit = 9
fr_api.set_flight_tracker_config(flight_tracker)
flights = fr_api.get_flights()


# B. TRANSFORM


# Structuring: fill lists then create table where columns = lists

for flight in fr_api.get_flights():
    details = fr_api.get_flight_details(flight.id)
    flight.set_flight_details(details)
    
    # manufacturer
    aircraft_code = flight.aircraft_code
    #print("aircraft_code : ", aircraft_code)

    # airline country
    airlines = fr_api.get_airlines()
    #for airline in airlines[0:1]:
        #print("airlines :", airline.keys())
        
    aircraft_model_code = fr_api.get_flight_details(flight)['aircraft']['model']['code']
    aircraft_model_name = fr_api.get_flight_details(flight)['aircraft']['model']['text']
    print("flight_details[airline] : ", aircraft_model_code)
    print("flight_details[airline] : ", aircraft_model_name)
    #print("flight_details.keys() : ", flight_details.keys())
    
    #print("flight_details[identification]: ", flight_details["identification"])
