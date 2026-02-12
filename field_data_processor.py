import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A class used to process field data from SQL databases and web-based CSVs.
    """

    def __init__(self, config_params, logging_level="INFO"):
        # Map attributes to the config_params dictionary
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """Sets up the logging configuration for the class."""
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """Ingests data from the SQL database using parameters from config."""
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """Swaps column names to correct mismatched data mapping."""
        # Extract keys and values for the swap
        column1 = list(self.columns_to_rename.keys())[0]
        column2 = list(self.columns_to_rename.values())[0]

        # Use a temporary name to avoid conflict during swap
        temp_name = "__temp_name_for_swap__"
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """Applies absolute value to Elevation and fixes Crop_type naming typos."""
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )

    def weather_station_mapping(self):
        """Fetches weather mapping data and merges it into the main DataFrame."""
        weather_map_df = read_from_web_CSV(self.weather_map_data)
        # Merging onto self.df allows the column to exist for the test cell
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
        return weather_map_df

    def process(self):
        """Runs the full data processing pipeline step-by-step."""
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()

        # Cleanup redundant columns
        if "Unnamed: 0" in self.df.columns:
            self.df = self.df.drop(columns="Unnamed: 0")

        self.logger.info("Step-by-step processing complete.")