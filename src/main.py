import os, datetime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit, array
from pyspark.sql import SparkSession
from pathlib import Path
import etl.data_reader as data_read
import etl.data_converter as data_converter


from settings.constants import *

def data_convertion(data_raw):
    data_processed = data_converter.DataConverter(data_raw.console_df,data_raw.result_df)
    # Changing "date" column to a date value
    data_processed.date_format()
    # Merging console and result dataframes to have all the fields in a single dataframe
    data_processed.merge_console_result()
    # Creating the dataframes with the top and worst values
    data_processed.generate_top_and_worst_10_console()
    data_processed.generate_top_and_worst_10_all()

    return data_processed
    

if __name__ == "__main__":

    # build spark session
    spark = (
        SparkSession.builder.appName("Console Data Reader")
        .getOrCreate()
        )

    # Reading the csv files with pre-defined schemas 
    data_raw = data_read.DataReader(spark)
    data_raw.get_console_data()
    data_raw.get_result_data()

    # Transformation of the data
    data_processed_df = data_convertion(data_raw)


    # Creating Paths to be able to write the dataframes
    Path(USERSCORE_REPORT_PATH).mkdir(parents=True, exist_ok=True)
    Path(METASCORE_REPORT_PATH).mkdir(parents=True, exist_ok=True)


    data_processed_df.top10_all_by_user.drop(data_processed_df.top10_all_by_user.columns[0], axis=1)
    #data_processed_df.result_df.write.option("header","true").mode('overwrite').csv("/data-processed/result")
    #data_processed_df.panda_result_df.to_csv("/data-processed/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.csv", sep=',', encoding='utf-8' , columns=header)
    
    try: 
        #Writing reports - ranked by Console and Company
        data_processed_df.top10_console_by_user.to_csv("/data-processed/reports-by-user-score/top10_by_console.csv", sep=',', encoding='utf-8')
        data_processed_df.top10_console_by_meta.to_csv("/data-processed/reports-by-meta-score/top10_by_console.csv", sep=',', encoding='utf-8')
        data_processed_df.worst10_console_by_user.to_csv("/data-processed/reports-by-user-score/worst10_by_console.csv", sep=',', encoding='utf-8')
        data_processed_df.worst10_console_by_meta.to_csv("/data-processed/reports-by-meta-score/worst10_by_console.csv", sep=',', encoding='utf-8')

        #Writing reports - ranked by all
        data_processed_df.top10_all_by_user.to_csv("/data-processed/reports-by-user-score/top10_all.csv", sep=',', encoding='utf-8' ,)
        data_processed_df.top10_all_by_meta.to_csv("/data-processed/reports-by-meta-score/top10_all.csv", sep=',', encoding='utf-8')
        data_processed_df.worst10_all_by_user.to_csv("/data-processed/reports-by-user-score/worst10_all.csv", sep=',', encoding='utf-8')
        data_processed_df.worst10_all_by_meta.to_csv("/data-processed/reports-by-meta-score/worst10_all.csv", sep=',', encoding='utf-8')
    except Exception as error:
        print ("There was an issue writing the reports "+error)
        