"""The present file aims to perform ETL pipeline on the FlightRadar24API data.
It is divided in three parts:
1. EXTRACT
2, TRANSFORM
3. LOAD
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

import pandas as pd
from FlightRadar24 import FlightRadar24API
from functions import (distance_between_airports,
                       continent_of_airport,
                       value_or_none,
                       get_current_time)


# 1. EXTRACT


# flight
flight_id = []
# airline
airline_name = []
airline_icao = []
# flight in process
flight_on_ground = []
# airports
airport_origin_name = []
airport_origin_icao= []

airport_destination_name = []
airport_destination_icao = []

# continent airports
airport_origin_continent = []
airport_destination_continent = []

# flight distance
distance_between_airports_ = []

# aircraft
aircraft_model = []
aircraft_manufacturer = []

# combining all information of flights
flight_info_list = []


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
    
    # flight
    tmp_flight_id = value_or_none(flight.id)
    flight_id.append(tmp_flight_id)

    tmp_flight_on_ground = value_or_none(flight.on_ground)
    flight_on_ground.append(tmp_flight_on_ground)

    # airline details
    tmp_airline_name = value_or_none(flight.airline_name)
    airline_name.append(tmp_airline_name)
    
    tmp_airline_icao = value_or_none(flight.airline_icao)
    airline_icao.append(tmp_airline_icao)
        
    # airports
    tmp_airport_origin_name = value_or_none(flight.origin_airport_name)
    airport_origin_name.append(tmp_airport_origin_name)
    
    tmp_airport_origin_icao = value_or_none(flight.origin_airport_icao)
    airport_origin_icao.append(tmp_airport_origin_icao)

    tmp_airport_destination_name = value_or_none(flight.destination_airport_name)
    airport_destination_name.append(tmp_airport_destination_name)
    
    tmp_airport_destination_icao = value_or_none(flight.destination_airport_icao)
    airport_destination_icao.append(tmp_airport_destination_icao)
    
    # continent airports
    tmp_airport_origin_continent = continent_of_airport(tmp_airport_origin_icao)
    airport_origin_continent.append(tmp_airport_origin_continent)

    tmp_airport_destination_continent = continent_of_airport(tmp_airport_destination_icao)
    airport_destination_continent.append(tmp_airport_destination_continent)

    # flight distance
    tmp_distance_between_airports_ = distance_between_airports(tmp_airport_origin_icao,
                                                                tmp_airport_destination_icao)
    distance_between_airports_.append(tmp_distance_between_airports_)

    # aircraft
    tmp_aircraft_model = value_or_none(flight.aircraft_model)
    aircraft_model.append(tmp_aircraft_model)
    
    tmp_aircraft_manufacturer = value_or_none(None)
    aircraft_manufacturer.append(tmp_aircraft_manufacturer)
    
    # overall info
    tmp_flight_info = (
        tmp_flight_id,
        tmp_flight_on_ground,
        tmp_airline_name,
        tmp_airline_icao,
        tmp_airport_origin_name,
        tmp_airport_origin_icao,
        tmp_airport_destination_name,
        tmp_airport_destination_icao,
        tmp_airport_origin_continent,
        tmp_airport_destination_continent,
        tmp_distance_between_airports_,
        tmp_aircraft_model,
        tmp_aircraft_manufacturer)
    flight_info_list.append(tmp_flight_info)


# create database

# create spark session
spark_session = SparkSession.builder.appName("spark_app").getOrCreate()

# define table schema

tab_schema = StructType([
    StructField("flight_id", StringType(), nullable=True),
    StructField("flight_on_ground", IntegerType(), nullable=True),
    StructField("airline_name", StringType(), nullable=True),
    StructField("airline_icao", StringType(), nullable=True),
    StructField("airport_origin_name", StringType(), nullable=True),
    StructField("airport_origin_icao", StringType(), nullable=True),
    StructField("airport_destination_name", StringType(), nullable=True),
    StructField("airport_destination_icao", StringType(), nullable=True),
    StructField("airport_origin_continent", StringType(), nullable=True),
    StructField("airport_destination_continent", StringType(), nullable=True),
    StructField("distance_between_airports", DoubleType(), nullable=True),
    StructField("aircraft_model", StringType(), nullable=True),
    StructField("aircraft_manufacturer", StringType(), nullable=True)
])

# create spark dataframe
data_df = spark_session.createDataFrame(data=flight_info_list,
                                        schema=tab_schema)

# 3. LOAD

# save spark dataframe
tab_dir = "data/"
current_datetime = get_current_time()
tab_name = f"flights_{current_datetime}"

# coalesce(1) save the dataframe in a single .csv file without partitioning it into mutiple files
# Spark saves the DataFrame as multiple files in a directory, not as a single CSV file. The reason for this is that Spark is designed to handle large datasets that might not fit into a single file,
data_df.coalesce(1).write.csv(tab_dir + tab_name, header=True)

# show table
# data_df.show()

# define data to be used in main.py
def get_current_data():
    return data_df

# def spark session to be used in main.py
def get_etl_sparksession():
    return spark_session

# stop spark session: should not be stopped cause used in main.py
# spark_session.stop()
