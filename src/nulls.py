from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sc = SparkContext()
spark = SparkSession(sc)

df: DataFrame = spark.createDataFrame(
    [
        {"expense": "meal", "cost": 100, "date": "2024-08-09", "paid": True},
        {"expense": "travel", "cost": 200, "date": "2024-08-10"},
        {"expense": "training", "cost": 300, "date": "2024-08-11", "paid": True},
        {"expense": "books", "cost": 400, "date": "2024-08-12"},
        {"expense": "meal", "cost": 300, "date": "2024-08-13", "paid": True},
        {"expense": "travel", "cost": 600, "date": "2024-08-14"},
        {"expense": "travel", "cost": 200, "date": "2024-08-15", "paid": True},
        {"expense": "training", "cost": 100, "date": "2024-08-16"},
    ]
)

# df with default paid value as false for null
df.fillna(False).show()  # df.na.fill(False)

# df which contaned only paid expenses 
df.dropna().show()  # df.na.drop()
