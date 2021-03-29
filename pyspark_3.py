from pyspark.sql import SparkSession

my_spark = SparkSession.builder.getOrCreate()

file_path = "./airports.csv"

airports = my_spark.read.csv(file_path, header=True)

result = airports.filter("latitude>=10").filter("latitude<=90").filter("longitude>=-90").filter("longitude<=-10")
result.show(result.count(), False)