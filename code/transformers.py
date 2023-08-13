"""Transform: Transform data after Extraction step"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from extractors import FlightExtractor


class FlightTransformer:
    def __init__(self):
        self.spark_session = SparkSession.builder.appName("spark_app").getOrCreate()

    def build_dataframe(self):
        # Extract data using the FlightExtractor class
        flight_extractor = FlightExtractor()
        current_flights_data_df = flight_extractor.extract_all_info_df()

        # Define table schema
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

        # Create Spark DataFrame
        data_df = self.spark_session.createDataFrame(data=current_flights_data_df,
                                                     schema=tab_schema)
        return data_df
