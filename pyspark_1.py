from pyspark.sql import SparkSession
import sys
outfile = sys.argv[1]

my_spark = SparkSession.builder.getOrCreate()

file_path = "./airports.csv"

airports = my_spark.read.csv(file_path, header=True)

result = airports.groupby("country").count()

result.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(outfile)