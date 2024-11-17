from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.csv("test.csv", sep="|", header=True)
df.show()
df1 = spark.read.load("test.csv", format="csv")
df2 = spark.read.load("test.orc", format="orc")
df3 = spark.read.load("test.parquet", format="parquet")
df4 = spark.read.load("test.json", format="json")
df4.cache()
