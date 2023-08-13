"""Functions needed in different files of the project"""

import os
from datetime import datetime
from FlightRadar24 import FlightRadar24API


# Extract: determine the distance between two airports
def distance_between_airports(origin_airport_icao=None, destination_airport_icao=None):
    """
    Calculate the distance between two airports using their ICAO codes.

    Parameters:
    origin_airport_icao (str): The ICAO code of the origin airport.
    destination_airport_icao (str): The ICAO code of the destination airport.

    Returns:
    float: The distance between the origin and destination airports in kilometers.

    Note:
    This function requires an active internet connection as it queries the FlightRadar24API
    to obtain airport information and calculates the distance between the two airports.
    If either the origin or destination airport ICAO code is not provided, the function
    returns None.
    """
    if origin_airport_icao and destination_airport_icao:
        fr_api = FlightRadar24API()
        airport_origin = fr_api.get_airport(origin_airport_icao)
        airport_destination = fr_api.get_airport(destination_airport_icao)
        distance_between_airports = airport_origin.get_distance_from(airport_destination)
        return distance_between_airports
    else:
        return None


# Extract: determine the continent of a given airport_icao
def continent_of_airport(airport_icao=None):
    """
    Get the continent of the airport using its ICAO code.

    Parameters:
        airport_icao (str): The ICAO code of the airport.

    Returns:
        str: The continent where the airport is located.

    Note:
        This function requires an active internet connection as it queries the FlightRadar24API
        to obtain airport details. If the airport ICAO code is not provided, the function returns None.
    """
    if airport_icao:
        fr_api = FlightRadar24API()
        time_zone_name = fr_api.get_airport_details(airport_icao)['airport']['pluginData']['details']['timezone']['name']
        continent = time_zone_name.split("/")[0]
        return continent
    else:
        return None


# Data Cleaning: 'N/A' replaced by None
def value_or_none(value):
    """
    Convert a given value to None if it is equal to 'N/A' or None.

    Parameters:
        value (any): The value to check.

    Returns:
        any: The original value if it is different from 'N/A' and None, otherwise returns None.
    """
    if value == 'N/A' or value == None:
        return None
    else:
        return value


# Data Cleaning: 'N/A' replaced by None inside a list 
def value_or_none_in_list(list_elements):
    """
    Convert a given value in a list to None if it is equal to 'N/A' or None.

    Parameters:
        value (any): The value to check.

    Returns:
        any: The original value if it is different from 'N/A' and None, otherwise returns None.
    """
    
    new_list = []
    for element in list_elements:
        if element == 'N/A' or element == None:
            new_list.append(None)
        else:
            new_list.append(element)
    
    return new_list


# Load: Time 
def get_current_time():
    """
    Get the current time and return it as a formatted string.

    Returns:
        str: The current time in the format "YYYYMMDDHHMMSS".
    """
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y%m%d%H%M%S")
    return formatted_time


# Load: Save dataframe as csv
def save_dataframe_to_csv(data_df, output_path):
    # Save the DataFrame to a CSV file
    data_df.write.csv(output_path, header=True, mode="overwrite")


# Report Generation
def write_row_by_row_tab(file_obj, col1, col2, col1_name, col2_name):
    """
    Writes a table with two columns, row by row, into the specified file object.

    Args:
        file_obj (file object): The file object to write the table into.
        col1 (list): The values of the first column.
        col2 (list): The values of the second column.
        col1_name (str): The name of the first column.
        col2_name (str): The name of the second column.

    Raises:
        AssertionError: If the lengths of col1 and col2 are not the same.

    Returns:
        None
    """
    # verify col1 and col2 are of the same length
    assert len(col1) == len(col2), "columns are not of the same length"
    # tab initialization: column name
    nb_rows = len(col1)
    # first row: column names
    file_obj.write(f"|{col1_name}|{col2_name}|\n")
    for i in range(nb_rows):
        # add note and its frequency as a new row in the table
        tmp_col1_value = col1[i]
        tmp_col2_value = col2[i]
        file_obj.write(f"| {tmp_col1_value} | {tmp_col2_value} | \n")


def now_flights_date_and_paths_dicts():
    # Get time
    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%Y-%m')
    day = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%Y.%m.%d.%H.%M.%S.%f')
    date_dict = {"year":year, "month":month, "day":day}
    
    # Paths 
    dir_path = "data/Flights/processed_data"
    partitioning = f"{{tech_year}}/{{tech_month}}/{{tech_day}}"
    file_name = f"flights{timestamp}.parquet"
    paths_dict = {"dir_path":dir_path, "partitioning":partitioning, "file_name":file_name}
    
    return date_dict, paths_dict
