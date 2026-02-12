import pandas as pd
import re
import logging
from data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    def __init__(self, config_params, logging_level="INFO"):
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        logger_name = __name__ + ".WeatherDataProcessor"
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
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.")

    def extract_measurement(self, message):
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                return key, float(next((x for x in match.groups() if x is not None)))
        return None, None

    def process_messages(self):
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'] = result.apply(lambda x: x[0])
            self.weather_df['Value'] = result.apply(lambda x: x[1])
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized.")
        return self.weather_df

    def calculate_means(self):
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            return None

    def process(self):
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Data processing completed.")