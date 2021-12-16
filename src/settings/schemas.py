
from pyspark.sql.types import *

from settings.constants import (  
    CONSOLE,
    COMPANY,
    METASCORE,
    NAME,
    USERSCORE,
    DATE,
)

CONSOLE_SCHEMA = StructType().add(CONSOLE,StringType(), True).add(COMPANY,StringType(), True)

RESULT_SCHEMA = StructType().add(METASCORE,IntegerType(), True).add(NAME,StringType(), True).add(CONSOLE,StringType(), True).add(USERSCORE,FloatType(), True).add(DATE,StringType(), True)
