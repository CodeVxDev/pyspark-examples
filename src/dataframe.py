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
    ]
)
df.createOrReplaceTempView("expenses")
df2 = spark.sql("SELECT expense, date from expenses where cost > 200")
df2.show()

df_expense = df[df.expense]
df_expense.show()
