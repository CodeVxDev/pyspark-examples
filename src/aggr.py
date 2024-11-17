from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD

sc = SparkContext()
spark = SparkSession(sc)

# With RDDs

# rdd: RDD = (
#     sc.parallelize([(300, "travel"), (100, "meal"), (200, "training")])
#     .sortByKey()
#     .collect()
# )
# print(rdd)

# With DataFrames
df: DataFrame = spark.createDataFrame(
    [
        {"expense": "meal", "cost": 100, "date": "2024-08-09"},
        {"expense": "travel", "cost": 200, "date": "2024-08-10"},
        {"expense": "training", "cost": 300, "date": "2024-08-11"},
        {"expense": "books", "cost": 400, "date": "2024-08-12"},
    ]
)

df.withColumn("pricey?", df.cost > 250).show()
