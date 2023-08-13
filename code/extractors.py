"""Extract: Extract data with the FlightRadar24API"""

import pandas as pd
from FlightRadar24 import FlightRadar24API
from functions import value_or_none_in_list

class FlightExtractor:
    def __init__(self, flight_tracker_limit):
        self.flight_tracker_limit = flight_tracker_limit

    def setting_flight_tracker_config(self):
        fr_api = FlightRadar24API()
        flight_tracker = fr_api.get_flight_tracker_config()
        flight_tracker.limit = self.flight_tracker_limit
        fr_api.set_flight_tracker_config(flight_tracker)
        return fr_api

    def extract_flights(self):
        fr_api = self.setting_flight_tracker_config()  
        flights = fr_api.get_flights()
        return flights


    # flight id, status, icao and name
    def extract_flight_id(self):
        flights = self.extract_flights()
        flight_id_vec = [flight.id for flight in flights]
        return value_or_none_in_list(flight_id_vec)
    
    def extract_flight_on_ground(self):
        flights = self.extract_flights()
        flights_on_ground_vec = [flight.on_ground for flight in flights]
        return value_or_none_in_list(flights_on_ground_vec)


    def extract_flights_details(self):
        fr_api = self.setting_flight_tracker_config()
        flights = self.extract_flights()
        list_flight_instance_details = []
        
        # extract details as instance of each flight
        for flight in flights:
            details = fr_api.get_flight_details(flight.id)
            flight.set_flight_details(details)
            list_flight_instance_details.append(flight)
        return list_flight_instance_details
            

    def extract_flight_airline_name(self):
        flights_details = self.extract_flights_details()
        airline_name_vec = [flight.airline_name for flight in flights_details]
        return value_or_none_in_list(airline_name_vec)
    
    def extract_flight_airline_icao(self):
        flights = self.extract_flights()
        airline_icao_vec = [flight.airline_icao for flight in flights]
        return value_or_none_in_list(airline_icao_vec)

    # airport name and icao 
    def extract_flight_origin_airport_name(self):
        flights = self.extract_flights_details()
        origin_airport_name = [flight.origin_airport_name for flight in flights]
        return value_or_none_in_list(origin_airport_name)
    
    def extract_flight_origin_airport_icao(self):
        flights = self.extract_flights_details()
        origin_airport_icao = [flight.origin_airport_icao for flight in flights]
        return value_or_none_in_list(origin_airport_icao)

    def extract_flight_destination_airport_name(self):
        flights = self.extract_flights_details()
        destination_airport_name = [flight.destination_airport_name for flight in flights]
        return value_or_none_in_list(destination_airport_name)

    def extract_flight_destination_airport_icao(self):
        flights = self.extract_flights_details()
        destination_airport_icao = [flight.destination_airport_icao for flight in flights]
        return value_or_none_in_list(destination_airport_icao)

    # continent of origin and destination
    def continent_of_airport_(self, airport_icao=None):
        """Get the continent of the airport using its ICAO code.
        """

        if airport_icao:
            fr_api = self.setting_flight_tracker_config()
            time_zone_name = fr_api.get_airport_details(airport_icao)['airport']['pluginData']['details']['timezone']['name']
            continent = time_zone_name.split("/")[0]
            return continent
        else:
            return None

    def extract_airport_origin_continent(self):
        airport_origin_icao_vec = self.extract_flight_origin_airport_icao()
        airport_origin_continent_vec = [self.continent_of_airport_(airport_icao=airport_origin_icao) for airport_origin_icao in airport_origin_icao_vec] 
        return value_or_none_in_list(airport_origin_continent_vec)
    
    def extract_airport_destination_continent(self):
        airport_destination_icao_vec = self.extract_flight_destination_airport_icao()
        airport_destination_continent_vec = [self.continent_of_airport_(airport_icao=airport_destination_icao) for airport_destination_icao in airport_destination_icao_vec] 
        return value_or_none_in_list(airport_destination_continent_vec)
    
    
    # distance between origin and destination airports 
    def compute_distance_between_airports(self, origin_airport_icao=None, destination_airport_icao=None):
        """Calculate the distance between two airports using their ICAO codes.
        """
        if origin_airport_icao and destination_airport_icao:
            fr_api = self.setting_flight_tracker_config()
            airport_origin = fr_api.get_airport(origin_airport_icao)
            airport_destination = fr_api.get_airport(destination_airport_icao)
            distance_between_airports = airport_origin.get_distance_from(airport_destination)
            return distance_between_airports
        else:
            return None

    def extract_destination_between_airports(self):
        origin_airport_icao_vec = self.extract_flight_origin_airport_icao()
        destination_airport_icao_vec = self.extract_flight_destination_airport_icao()
        
        destination_between_airports_vec = [self.compute_distance_between_airports(origin_airport_icao, destination_airport_icao) for origin_airport_icao, destination_airport_icao in zip(origin_airport_icao_vec, destination_airport_icao_vec)]

        return value_or_none_in_list(destination_between_airports_vec)


    # aircraft
    def extract_aircraft_model(self):
        flights = self.extract_flights_details()
        aircraft_model_vec = [flight.aircraft_model for flight in flights]
        return value_or_none_in_list(aircraft_model_vec)

    def extract_aircraft_manufacturer(self):
        flights = self.extract_flights_details()
        aircraft_manufacturer_vec = [None for _ in flights]
        return value_or_none_in_list(aircraft_manufacturer_vec)


    def extract_all_info_df(self):
        data_cols_in_list={
            "flight_id": self.extract_flight_id(),
            "flight_on_ground": self.extract_flight_on_ground(),
            "airline_name": self.extract_flight_airline_name(),
            "airline_icao": self.extract_flight_airline_icao(),
            "airport_origin_name": self.extract_flight_origin_airport_name(),
            "airport_origin_icao": self.extract_flight_origin_airport_icao(),
            "airport_destination_name": self.extract_flight_destination_airport_name(),
            "airport_destination_icao": self.extract_flight_destination_airport_icao(),
            "airport_origin_continent": self.extract_airport_origin_continent(),
            "airport_destination_continent": self.extract_airport_destination_continent(),
            "distance_between_airports": self.compute_distance_between_airports(),
            "aircraft_model": self.extract_aircraft_model(),
            "aircraft_manufacturer": self.extract_aircraft_manufacturer() 
        }
        
        return pd.DataFrame(data_cols_in_list)
