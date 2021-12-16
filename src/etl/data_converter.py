from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame as df
from pyspark.sql.types import TimestampType, IntegerType
import pandas as panda

from settings.constants import *

class DataConverter:


    def __init__(self,console_df,result_df):
        self.console_df = console_df
        self.result_df = result_df
        self.panda_console_result_df = panda.DataFrame()
        self.panda_console_df = panda.DataFrame()
        self.panda_result_df = panda.DataFrame()

        self.console_ranking =panda.DataFrame()
        self.top10_console_by_user=panda.DataFrame()
        self.top10_console_by_meta=panda.DataFrame()
        self.worst10_console_by_user=panda.DataFrame()
        self.worst10_console_by_meta=panda.DataFrame()
        self.all_ranking =panda.DataFrame()
        self.top10_all_by_user=panda.DataFrame()
        self.top10_all_by_meta=panda.DataFrame()
        self.worst10_all_by_user=panda.DataFrame()
        self.worst10_all_by_meta=panda.DataFrame()

        self.panda_console_df = self.console_df.toPandas()
        self.panda_console_df = self.panda_console_df.rename(columns = {"" : "Signal"})

    def date_format(self):
        self.panda_result_df = self.result_df.toPandas()
        self.panda_result_df = self.panda_result_df.rename(columns = {"" : "Signal"})
        self.panda_result_df[DATE] = panda.to_datetime(self.panda_result_df['date'], format="%b %d, %Y", errors='coerce')

    def merge_console_result(self):
        self.panda_console_result_df = self.panda_result_df.merge( self.panda_console_df, how='inner', on=CONSOLE)

    def generate_top_and_worst_10_console(self):
        self.console_ranking = self.panda_console_result_df.groupby([COMPANY,CONSOLE])[NAME, USERSCORE , METASCORE, DATE]
        self.top10_console_by_user = self.console_ranking.apply(lambda x: x.nlargest(10, columns=USERSCORE)).copy()
        self.top10_console_by_meta = self.console_ranking.apply(lambda x: x.nlargest(10, columns=METASCORE)).copy()
        self.worst10_console_by_user = self.console_ranking.apply(lambda x: x.nsmallest(10, columns=USERSCORE)).copy()
        self.worst10_console_by_meta = self.console_ranking.apply(lambda x: x.nsmallest(10, columns=METASCORE)).copy()

    def generate_top_and_worst_10_all(self):
        self.all_ranking = self.panda_console_result_df[[COMPANY, CONSOLE, NAME, USERSCORE , METASCORE, DATE]]
        self.top10_all_by_user = self.all_ranking.nlargest(10, USERSCORE).copy()
        self.top10_all_by_meta = self.all_ranking.nlargest(10, METASCORE).copy()
        self.worst10_all_by_user = self.all_ranking.nsmallest(10, USERSCORE).copy()
        self.worst10_all_by_meta = self.all_ranking.nsmallest(10, METASCORE).copy()
        
