from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, from_json
from pyspark.sql.types import StructField, StringType, StructType

# tạm thời cứ để đấy