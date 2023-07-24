import datetime
from FlightRadar24 import FlightRadar24API


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


# 'N/A' replaced by None
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


def get_current_time():
    """
    Get the current time and return it as a formatted string.

    Returns:
        str: The current time in the format "YYYYMMDDHHMMSS".
    """
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y%m%d%H%M%S")
    return formatted_time


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
