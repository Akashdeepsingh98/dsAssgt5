from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_spark = SparkSession.builder.getOrCreate()

file_path = "./airports.csv"

airports = my_spark.read.csv(file_path, header=True)

airports.groupby(airports["country"]).count().sort(col('count').desc()).limit(1).show()