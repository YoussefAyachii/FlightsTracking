"""Load: Save data as csv after Extract and Transform steps"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from transformers import FlightTransformer
from functions import now_flights_date_and_paths_dicts


class NowFlightsDataFrame():

    def __init__(self, spark_session, flight_tracker_limit):
        # limit of nb flights to return (due to FlightTrackerAPI constraints)
        self.flight_tracker_limit = flight_tracker_limit

        # Create a Spark session
        self.spark_session = spark_session
        self.now_flights_df = FlightTransformer(self.spark_session, self.flight_tracker_limit).build_dataframe()
        self.date_dict, self.paths_dict = now_flights_date_and_paths_dicts()

        # save data
        # create dir if does not exist
        os.makedirs(f"{self.paths_dict['dir_path']}/{self.paths_dict['partitioning']}", exist_ok=True)

        # Add new columns to use for partitioning: year, month, day
        data_with_partition_cols = self.now_flights_df.withColumn("tech_year", lit(self.date_dict['year'])) \
            .withColumn("tech_month", lit(self.date_dict['month'])) \
            .withColumn("tech_day", lit(self.date_dict['day']))

        # Save dataframe
        data_with_partition_cols.write.partitionBy(["tech_year", "tech_month", "tech_day"]) \
            .parquet(f"{self.paths_dict['dir_path']}/{self.paths_dict['partitioning']}/{self.paths_dict['file_name']}")
