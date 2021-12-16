
from pyspark.sql.dataframe import DataFrame as df
from pathlib import Path

from settings.schemas import ( 
    RESULT_SCHEMA,
    CONSOLE_SCHEMA,
)

from settings.constants import (  
    CONSOLES_DATA_PATH,
    RESULT_DATA_PATH,
    CONSOLES_FORMAT,
    RESULT_FORMAT,
)

class DataReader:


    def __init__(self,spark):
        self.console_df = df(None, None)
        self.result_df = df(None, None)
        self.spark_session = spark

    def get_console_data(self):

        self.console_df = self.spark_session.read.load(CONSOLES_DATA_PATH,format=CONSOLES_FORMAT, header="true" , schema=CONSOLE_SCHEMA)

    def get_result_data(self):

        self.result_df = self.spark_session.read.load(RESULT_DATA_PATH,format=RESULT_FORMAT, header="true" , schema=RESULT_SCHEMA)

