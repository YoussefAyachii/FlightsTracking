"""Running the present file will run the etl.py file to get
an update of the flightradar24 dataset. Then it will perform
some specified queries and show the results within the 
./results.txt file."""


from etl import get_current_data, get_etl_sparksession
from functions import write_row_by_row_tab


# I. QUERIES AND EXECUTION

# create spark session
#spark_session = SparkSession.builder.appName("spark_app").getOrCreate()
spark_session = get_etl_sparksession()

# load new data
def main():
    data = get_current_data()

if __name__ == "__main__":
    main()


# load dataset
data = get_current_data()
data.show()


# Register DataFrame as a temporary table
data.createOrReplaceTempView("data_table")

# ===========================
# QUERIES 

# 1. La compagnie avec le + de vols en cours
airline_with_most_flights_inprogress_query = f"""
SELECT airline_name, COUNT(airline_icao) AS nb_flights_in_progress
    FROM data_table
    GROUP BY airline_name
    ORDER BY nb_flights_in_progress DESC
    LIMIT 1
"""

airline_with_most_current_flight_df = spark_session.sql(airline_with_most_flights_inprogress_query)
airline_with_most_current_flight_df.show()

# 2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
airline_with_most_current_flights_per_continent_query = f"""
SELECT airline_name, airport_origin_continent AS continent, COUNT(airline_name) AS number_of_intracontinent_flights
    FROM data_table
        WHERE flight_on_ground = 0
        AND airport_origin_continent = airport_destination_continent
    GROUP BY continent, airline_name
"""

airline_with_most_current_flights_per_continent_df = spark_session.sql(airline_with_most_current_flights_per_continent_query)
airline_with_most_current_flights_per_continent_df.show()



# 3. Le vol en cours avec le trajet le plus long
current_flight_with_longest_trip_query = f"""
SELECT flight_id, airline_name, airport_origin_name, airport_destination_name, distance_between_airports
    FROM data_table
    ORDER BY distance_between_airports DESC
    LIMIT 1
"""

current_flight_with_longest_trip_df = spark_session.sql(current_flight_with_longest_trip_query)
current_flight_with_longest_trip_df.show()

# 4. Pour chaque continent, la longueur de vol moyenne
average_trip_distance_per_continent_query = f"""
SELECT airport_origin_continent AS continent, AVG(distance_between_airports) AS avg_flight_distance
    FROM data_table
    WHERE airport_origin_continent = airport_destination_continent
    GROUP BY continent
"""

average_trip_distance_per_continent_df = spark_session.sql(average_trip_distance_per_continent_query)
average_trip_distance_per_continent_df.show()

# 5. L'entreprise constructeur d'avions avec le plus de vols actifs
# Did not found the Manufacturer information. Suppose it exists, we would do:
aircraft_manufacturer_with_most_actif_trips_query = f"""
SELECT aircraft_manufacturer, COUNT(flight_on_ground = 0) AS nb_active_flights
    FROM data_table
    WHERE flight_on_ground = 0
    GROUP BY aircraft_manufacturer
    ORDER BY nb_active_flights DESC
    LIMIT 1
"""

aircraft_manufacturer_with_most_active_trips_df = spark_session.sql(aircraft_manufacturer_with_most_actif_trips_query)
aircraft_manufacturer_with_most_active_trips_df.show()

# ==========

# 6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
# Did not found the country information of each airline.
# Supposing that the country info is sroted as airline_country column, the query would be:

#top_3_aircraft_models_per_airline_country_query = f"""
#SELECT airline_country, aircraft_model, SUM(COUNT(aircraft_model)) AS nb_aircrafts
#    FROM data_table
#    GROUP BY airline_country, aircraft_model
#    ORDER BY nb_aircrafts DESC
#"""

#top_3_aircraft_models_per_airline_country_df = spark_session.sql(top_3_aircraft_models_per_airline_country_query)
#top_3_aircraft_models_per_airline_country_df.show()

# ===========

# Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?
airport_with_most_diffce_between_in_and_out_flights_query = f"""
SELECT in.airport_destination_name AS airport, ABS(COUNT(in.airport_destination_name) - COUNT(out.airport_origin_name)) AS abs_diff_in_out_flights
    FROM data_table AS in
    FULL JOIN data_table AS out
    ON in.airport_destination_name = out.airport_origin_name
    WHERE in.airport_destination_name IS NOT NULL
    GROUP BY airport
    ORDER BY abs_diff_in_out_flights DESC
    LIMIT 1
"""

airport_with_most_diff_in_out_flights_df = spark_session.sql(airport_with_most_diffce_between_in_and_out_flights_query)
airport_with_most_diff_in_out_flights_df.show()

# II. REPORT GENERATION

with open("./results.txt", "w", encoding="utf8") as f:
    # report title
    f.write("=== Résultats répondant aux énoncés du README.md === \n")
    # introduction
    f.write("\nLe présent fichier a été généré par l'exécution du fichier code/main.py.\n\n")
    
    # 1. La compagnie avec le + de vols en cours
    try:
        airline_with_most_current_flight_row = airline_with_most_current_flight_df.take(1)[0]
        airline_name = airline_with_most_current_flight_row["airline_name"]
        nb_flights_in_progress = airline_with_most_current_flight_row["nb_flights_in_progress"]

        f.write(f"1. La compagnie avec le + de vols en cours est "
                f"{airline_name} avec {nb_flights_in_progress} vols en cours.\n\n")
    except IndexError:
        # case when the resulting table is empty
        f.write("1. Par manque de donnés, nous ne pouvons pas déterminer la compagnie avec le + de vols en cours.\n\n")

    # 2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
    try:
        airline_with_most_current_flights_per_continent_row = airline_with_most_current_flights_per_continent_df.take(1)[0]
        airline_name_per_continent = airline_with_most_current_flights_per_continent_row["airline_name"]
        number_of_intracontinent_flights = airline_with_most_current_flights_per_continent_row["number_of_intracontinent_flights"]
        continent = airline_with_most_current_flights_per_continent_row["continent"]

        f.write(f"2. la compagnie avec le + de vols régionaux actifs est "
                f"{airline_name_per_continent} avec {number_of_intracontinent_flights} vols intraregionaux dans qui se déroulent en {continent}.\n\n")
    except IndexError:
        # case when the resulting table is empty
        f.write("2. Par manque de donnés, nous ne pouvons pas déterminer la compagnie avec le + de vols régionaux actifs.\n\n")

    # 3. Le vol en cours avec le trajet le plus long
    try:
        current_flight_with_longest_trip_row = current_flight_with_longest_trip_df.take(1)[0]
        flight_id_longest_trip = current_flight_with_longest_trip_row["flight_id"]
        airline_name_longest_trip = current_flight_with_longest_trip_row["airline_name"]
        airport_origin_name_longest_trip = current_flight_with_longest_trip_row["airport_origin_name"]
        airport_destination_name_longest_trip = current_flight_with_longest_trip_row["airport_destination_name"]
        distance_between_airports_longest_trip = current_flight_with_longest_trip_row["distance_between_airports"]

        f.write(f"3. Le vol en cours avec le trajet le plus long est le vol {flight_id_longest_trip}"
                f" de l'airline {airline_name_longest_trip} qui part de \n {airport_origin_name_longest_trip}"
                f" à {airport_destination_name_longest_trip}. La distance entre les deux aéroports est de"
                f" {distance_between_airports_longest_trip}.\n\n")

    except IndexError:
        # case when the resulting table is empty
        f.write("3. Par manque de donnés, nous ne pouvons pas déterminer le vol en cours avec le trajet le plus long.\n\n")

    # 4. Pour chaque continent, la longueur de vol moyenne
    f.write("4. Longueur moyenne de vol pour chaque continent:\n")
    try:
        continents_list = [element["continent"] for element in average_trip_distance_per_continent_df.collect()]
        avg_flight_distance_list = [element["avg_flight_distance"] for element in average_trip_distance_per_continent_df.collect()]
        write_row_by_row_tab(file_obj=f,
                            col1=continents_list,
                            col2=avg_flight_distance_list,
                            col1_name="continent",
                            col2_name="avg_flight_distance")
        f.write("\n")
    except IndexError:
        # case when the resulting table is empty
        f.write("4. Par manque de donnés, nous ne pouvons pas déterminer la longueur de vol moyenne pour chaque continent.\n\n")

    # 5. L'entreprise constructeur d'avions avec le plus de vols actifs
    try:
        aircraft_manufacturer_with_most_active_trips_row = aircraft_manufacturer_with_most_active_trips_df.take(1)[0]
        manufacturer_name_most_active_trips = aircraft_manufacturer_with_most_active_trips_row["aircraft_manufacturer"]
        nb_active_flights = aircraft_manufacturer_with_most_active_trips_row["nb_active_flights"]

        f.write(f"5. L'entreprise constructeur d'avions avec le plus de vols actifs est {manufacturer_name_most_active_trips}"
                f" avec {nb_active_flights} vols actifs.\n\n")

    except IndexError:
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
        airport_with_most_diff_in_out_flights_row = airport_with_most_diff_in_out_flights_df.take(1)[0]
        airport_name_most_diff_in_out_flights = airport_with_most_diff_in_out_flights_row["airport"]
        abs_diff_in_out_flights = airport_with_most_diff_in_out_flights_row["abs_diff_in_out_flights"]

        f.write(f"Bonus. L'aéroport avec la plus grande différence entre le nombre de vols sortants et le nombre de vols entrants \n est {airport_name_most_diff_in_out_flights}"
                f" avec une différence absolue de {abs_diff_in_out_flights}.\n\n")

    except IndexError:
        # case when the resulting table is empty
        f.write("Bonus: Par manque de données, nous ne pouvons pas déterminer l'aéroport avec la plus grande différence entre \nle nombre de vols sortants et le nombre de vols entrants.\n")

    f.close()

# stop spark session:
spark_session.stop()
