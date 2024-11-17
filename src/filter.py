from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

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

df_avg: DataFrame = (
    df.groupBy("expense").agg(F.avg("cost").alias("average_expense"))
).join(df.groupBy("expense").agg(F.sum("cost").alias("total_expense")), on="expense")

df_avg.show()
