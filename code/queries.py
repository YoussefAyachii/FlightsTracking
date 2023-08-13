"""Defining queries and methods to execute them within a spark session"""


from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, ParseException


class FlightQueries:
    def __init__(self, spark_session, now_flights_df):

        self.spark_session = spark_session
        self.now_flights_df = now_flights_df
        
        # register datatable
        self.now_flights_df.createOrReplaceTempView("flights_tab")

        # 1. La compagnie avec le + de vols en cours
        self.airline_with_most_flights_inprogress_query = f"""
        SELECT airline_name, COUNT(*) AS nb_flights_in_progress
        FROM flights_tab
        WHERE flight_on_ground = 0
        AND airline_name IS NOT NULL
        GROUP BY airline_name
        ORDER BY nb_flights_in_progress DESC
        LIMIT 1
        """

        # 2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
        self.airline_with_most_current_flights_per_continent_query = f"""
        SELECT airline_name, airport_origin_continent AS continent, COUNT(airline_name) AS number_of_intracontinent_flights
            FROM flights_tab
            WHERE airline_name IS NOT NULL
            AND airport_origin_continent IS NOT NULL
            AND flight_on_ground = 0
            AND airport_origin_continent = airport_destination_continent
            GROUP BY continent, airline_name
        """

        # 3. Le vol en cours avec le trajet le plus long
        self. current_flight_with_longest_trip_query = f"""
        SELECT flight_id, airline_name, airport_origin_name, airport_destination_name, distance_between_airports
            FROM flights_tab
            WHERE COALESCE (flight_id, airline_name, airport_origin_name, airport_destination_name, distance_between_airports) IS NOT NULL
            ORDER BY distance_between_airports DESC
            LIMIT 1
        """
        
        # 4. Pour chaque continent, la longueur de vol moyenne
        self.average_trip_distance_per_continent_query = f"""
        SELECT airport_origin_continent AS continent, AVG(distance_between_airports) AS avg_flight_distance
            FROM flights_tab
            WHERE airport_origin_continent IS NOT NULL
            AND distance_between_airports IS NOT NULL
            AND airport_origin_continent = airport_destination_continent
            GROUP BY continent
        """
        
        # 5. L'entreprise constructeur d'avions avec le plus de vols actifs
        # Did not found the Manufacturer information. Suppose it exists, we would do:
        self.aircraft_manufacturer_with_most_actif_trips_query = f"""
        SELECT aircraft_manufacturer, COUNT(flight_on_ground = 0) AS nb_active_flights
            FROM flights_tab
            WHERE aircraft_manufacturer IS NOT NULL
            AND flight_on_ground = 0
            GROUP BY aircraft_manufacturer
            ORDER BY nb_active_flights DESC
            LIMIT 1
        """

        # ==========

        # 6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
        # Did not found the country information of each airline.
        # Supposing that the country info is sroted as airline_country column, the query would be:

        #top_3_aircraft_models_per_airline_country_query = f"""
        #SELECT airline_country, aircraft_model, SUM(COUNT(aircraft_model)) AS nb_aircrafts
        #    FROM {self.flights_tab}
        #    GROUP BY airline_country, aircraft_model
        #    ORDER BY nb_aircrafts DESC
        #"""

        #top_3_aircraft_models_per_airline_country_df = spark_session.sql(top_3_aircraft_models_per_airline_country_query)
        #top_3_aircraft_models_per_airline_country_df.show()

        # ===========

        # 7. Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?
        self.airport_with_most_diffce_between_in_and_out_flights_query = f"""
        SELECT in.airport_destination_name AS airport, ABS(COUNT(in.airport_destination_name) - COUNT(out.airport_origin_name)) AS abs_diff_in_out_flights
            FROM flights_tab AS in
            FULL JOIN flights_tab AS out
            ON in.airport_destination_name = out.airport_origin_name
            WHERE in.airport_destination_name IS NOT NULL
            AND out.airport_origin_name IS NOT NULL
            GROUP BY airport
            ORDER BY abs_diff_in_out_flights DESC
            LIMIT 1
        """


    # Query execution method with error handling
    def execute_query(self, query):
        try:
            query_output_df = self.spark_session.sql(query)
            return query_output_df
        except ParseException as e:
            print(f"Syntax error in query: {query}")
            print(f"ParseException message: {e}")
            return None
        except AnalysisException as e:
            print(f"Analysis error in query: {query}")
            print(f"AnalysisException message: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    # Methods for getting the query results with error handling
    
    # 1. La compagnie avec le + de vols en cours
    def get_query_airline_with_most_flights_inprogress(self):
        return self.execute_query(self.airline_with_most_flights_inprogress_query)

    # 2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
    def get_query_airline_with_most_current_flights_per_continent(self):
        return self.execute_query(self.airline_with_most_current_flights_per_continent_query)

    # 3. Le vol en cours avec le trajet le plus long
    def get_query_current_flight_with_longest_trip(self):
        return self.execute_query(self.current_flight_with_longest_trip_query)

    # 4. Pour chaque continent, la longueur de vol moyenne
    def get_query_average_trip_distance_per_continent(self):
        return self.execute_query(self.average_trip_distance_per_continent_query)

    # 5. L'entreprise constructeur d'avions avec le plus de vols actifs
    # Did not found the Manufacturer information. Suppose it exists, we would do:
    def get_query_aircraft_manufacturer_with_most_actif_trips(self):
        return self.execute_query(self.aircraft_manufacturer_with_most_actif_trips_query)

    # 6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
    # Did not found the country information of each airline.
    #def get_query_top_3_aircraft_models_per_airline_country(self):
    #    return self.top_3_aircraft_models_per_airline_country_query

    # 7. Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?
    def get_query_airport_with_most_diffce_between_in_and_out_flights(self):
        return self.execute_query(self.airport_with_most_diffce_between_in_and_out_flights_query)
