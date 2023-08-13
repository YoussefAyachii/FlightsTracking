"""Load: Save data as csv after Extract and Transform steps"""

import os
from datetime import datetime
from pyspark.sql.functions import col, lit

from transformers import FlightTransformer
from functions import save_dataframe_to_csv, get_current_time, generate_save_path


if __name__ == "__main__":
    
    # Import data
    flight_transformer = FlightTransformer()
    data_df = flight_transformer.build_dataframe()

    # Partitioning by tech_year, tech_month, and tech_day
    
    # Get time
    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%Y-%m')
    day = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%Y.%m.%d.%H.%M.%S.%f')

    # Add new columns to use for partitioning: year, month, day
    data_with_partition_cols = data_df.withColumn("tech_year", lit(year)) \
        .withColumn("tech_month", lit(month)) \
        .withColumn("tech_day", lit(day))

    # Load
    
    # Paths 
    dir_path = "data/Flights/processed_data"
    partitioning = f"{{tech_year}}/{{tech_month}}/{{tech_day}}"
    file_name = f"flights{timestamp}.parquet"

    # Create directory if does not exist
    os.makedirs(f"{dir_path}/{partitioning}", exist_ok=True)

    # Save data
    data_with_partition_cols.write.partitionBy(["tech_year", "tech_month", "tech_day"]) \
        .parquet(f"{dir_path}/{partitioning}/{file_name}")
