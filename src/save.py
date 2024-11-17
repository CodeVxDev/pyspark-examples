from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

sc = SparkContext()
spark = SparkSession(sc)

df: DataFrame = spark.createDataFrame(
    [
        {"expense": "meal", "cost": 100, "date": "2024-08-09"},
        {"expense": "travel", "cost": 200, "date": "2024-08-10"},
        {"expense": "training", "cost": 300, "date": "2024-08-11"},
        {"expense": "books", "cost": 400, "date": "2024-08-12"},
        {"expense": "meal", "cost": 300, "date": "2024-08-13"},
        {"expense": "travel", "cost": 600, "date": "2024-08-14"},
        {"expense": "travel", "cost": 200, "date": "2024-08-15"},
        {"expense": "training", "cost": 100, "date": "2024-08-16"},
    ]
)

df.write.csv("test1.csv", sep="|", header=True)
spark.write.save("test2.csv", format="csv")
spark.write.save("test.orc", format="orc")
spark.write.save("test.parquet", format="parquet")
spark.write.save("test.json", format="json")
