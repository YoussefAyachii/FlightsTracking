from pyspark.sql import SparkSession

from queries import FlightQueries
from extractors import FlightExtractor
from transformers import FlightTransformer
from load import NowFlightsDataFrame
from functions import write_row_by_row_tab


# set number of flights to track (due to FlightTrackerAPI constraints)
nb_flights = 11


if __name__ == "__main__":
    # create spark session
    spark_session = SparkSession.builder.appName("MainApp").getOrCreate()

    # get flights processed data (post ETL) as a spark dataframe
    now_flights = NowFlightsDataFrame(spark_session, flight_tracker_limit=nb_flights)
    now_flights_df = now_flights.now_flights_df
    now_flights_df.show()


    queries_manager = FlightQueries(spark_session=spark_session,
                                    now_flights_df=now_flights_df)


    # REPORT GENERATION

    with open("./results.txt", "w", encoding="utf8") as f:
        # report title
        f.write("=== Résultats répondant aux énoncés du README.md === \n")
        # introduction
        f.write("\nLe présent fichier a été généré par l'exécution du fichier code/main.py.\n\n")
        
        # 1. La compagnie avec le + de vols en cours
        try:
            airline_with_most_current_flight_df = queries_manager.get_query_airline_with_most_flights_inprogress()
            airline_with_most_current_flight_row = airline_with_most_current_flight_df.first()
            airline_name = airline_with_most_current_flight_row.airline_name
            nb_flights_in_progress = airline_with_most_current_flight_row.nb_flights_in_progress

            f.write(f"1. La compagnie avec le + de vols en cours est "
                    f"{airline_name} avec {nb_flights_in_progress} vols en cours.\n\n")
        except AttributeError:
            # case when the resulting table is empty
            f.write("1. Par manque de donnés, nous ne pouvons pas déterminer la compagnie avec le + de vols en cours.\n\n")

        
        # 2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
        try:
            airline_with_most_current_flights_per_continent_df = queries_manager.get_query_airline_with_most_current_flights_per_continent()  
            airline_with_most_current_flights_per_continent_row = airline_with_most_current_flights_per_continent_df.first()
            airline_with_most_current_flights_per_continent_df.show()

            airline_name_per_continent = airline_with_most_current_flights_per_continent_row.airline_name
            number_of_intracontinent_flights = airline_with_most_current_flights_per_continent_row.number_of_intracontinent_flights
            continent = airline_with_most_current_flights_per_continent_row.continent

            f.write(f"2. la compagnie avec le + de vols régionaux actifs est " +
                    f"{airline_name_per_continent} avec {number_of_intracontinent_flights} vols intraregionaux " +
                    f"dans qui se déroulent en {continent}.\n\n")
        except AttributeError:
            # case when the resulting table is empty, .i.e. empty first row of airline_with_most_current_flights_per_continent_df
            f.write("2. Par manque de donnés, nous ne pouvons pas déterminer la compagnie avec le + de vols régionaux actifs.\n\n")


        # 3. Le vol en cours avec le trajet le plus long
        try:
            current_flight_with_longest_trip_df = queries_manager.get_query_current_flight_with_longest_trip()
            current_flight_with_longest_trip_row = current_flight_with_longest_trip_df.first()
    
            flight_id_longest_trip = current_flight_with_longest_trip_row.flight_id
            airline_name_longest_trip = current_flight_with_longest_trip_row.airline_name
            airport_origin_name_longest_trip = current_flight_with_longest_trip_row.airport_origin_name
            airport_destination_name_longest_trip = current_flight_with_longest_trip_row.airport_destination_name
            distance_between_airports_longest_trip = current_flight_with_longest_trip_row.distance_between_airports

            f.write(f"3. Le vol en cours avec le trajet le plus long est le vol {flight_id_longest_trip}"
                    f" de l'airline {airline_name_longest_trip} qui part de \n {airport_origin_name_longest_trip}"
                    f" à {airport_destination_name_longest_trip}. La distance entre les deux aéroports est de"
                    f" {distance_between_airports_longest_trip}.\n\n")

        except AttributeError:
            # case when the resulting table is empty
            f.write("3. Par manque de donnés, nous ne pouvons pas déterminer le vol en cours avec le trajet le plus long.\n\n")

        # 4. Pour chaque continent, la longueur de vol moyenne
        f.write("4. Longueur moyenne de vol pour chaque continent:\n")
        try:
            average_trip_distance_per_continent_df = queries_manager.get_query_average_trip_distance_per_continent()

            # get all elements in the "continent" column
            continents_list = [element["continent"] for element in average_trip_distance_per_continent_df.collect()]
            avg_flight_distance_list = [element["avg_flight_distance"] for element in average_trip_distance_per_continent_df.collect()]
            write_row_by_row_tab(file_obj=f,
                                col1=continents_list,
                                col2=avg_flight_distance_list,
                                col1_name="continent",
                                col2_name="avg_flight_distance")
            f.write("\n")
        except AttributeError:
            # case when the resulting table is empty
            f.write("4. Par manque de donnés, nous ne pouvons pas déterminer la longueur de vol moyenne pour chaque continent.\n\n")



        # 5. L'entreprise constructeur d'avions avec le plus de vols actifs
        try:
            aircraft_manufacturer_with_most_active_trips_df = queries_manager.get_query_aircraft_manufacturer_with_most_actif_trips()

            aircraft_manufacturer_with_most_active_trips_row = aircraft_manufacturer_with_most_active_trips_df.first()
            manufacturer_name_most_active_trips = aircraft_manufacturer_with_most_active_trips_row.aircraft_manufacturer
            nb_active_flights = aircraft_manufacturer_with_most_active_trips_row.nb_active_flights

            f.write(f"5. L'entreprise constructeur d'avions avec le plus de vols actifs est {manufacturer_name_most_active_trips}"
                    f" avec {nb_active_flights} vols actifs.\n\n")

        except AttributeError:
            # case when the resulting table is empty
            f.write("5. Par manque de données, nous ne pouvons pas déterminer l'entreprise constructeur d'avions avec le plus de vols actifs.\n\n")

        # =============
        # 6 Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
        #try:
        #    f.write(f"""6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage:\n""")
        #    airline_country_list = [element["airline_country"] for element in top_3_aircraft_models_per_airline_country_df.collect()]
        #    aircraft_model_list = [element["aircraft_model"] for element in top_3_aircraft_models_per_airline_country_df.collect()]
        #    write_row_by_row_tab(file_obj=f,
        #                        col1=airline_country_list,
        #                        col2=aircraft_model_list,
        #                        col1_name="airline_country",
        #                        col2_name="aircraft_model")
        #except IndexError:
        #    f.write("""6. Par manque de donnés, nous ne pouvons pas déterminer pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage.\n""")
        # =============

        # Bonus:
        try:
            airport_with_most_diff_in_out_flights_df = queries_manager.get_query_airport_with_most_diffce_between_in_and_out_flights()
            airport_with_most_diff_in_out_flights_row = airport_with_most_diff_in_out_flights_df.first()
    
            airport_name_most_diff_in_out_flights = airport_with_most_diff_in_out_flights_row.airport
            abs_diff_in_out_flights = airport_with_most_diff_in_out_flights_row.abs_diff_in_out_flights

            f.write(f"Bonus. L'aéroport avec la plus grande différence entre le nombre de vols sortants et le nombre de vols entrants \n est {airport_name_most_diff_in_out_flights}"
                    f" avec une différence absolue de {abs_diff_in_out_flights}.\n\n")

        except AttributeError:
            # case when the resulting table is empty
            f.write("Bonus: Par manque de données, nous ne pouvons pas déterminer l'aéroport avec la plus grande différence entre \nle nombre de vols sortants et le nombre de vols entrants.\n")

        # close results.txt file
        f.close()

    # Stop spark session
    spark_session.stop()
