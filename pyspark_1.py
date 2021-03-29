from pyspark.sql import SparkSession

my_spark = SparkSession.builder.getOrCreate()

file_path = "./airports.csv"

airports = my_spark.read.csv(file_path, header=True)

airports.groupby("country").count().show()