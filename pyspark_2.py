from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
outfile = sys.argv[1]

my_spark = SparkSession.builder.getOrCreate()

file_path = "./airports.csv"

airports = my_spark.read.csv(file_path, header=True)

result = airports.groupby(airports["country"]).count().sort(col('count').desc()).limit(1)

result.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(outfile)