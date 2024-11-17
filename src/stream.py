from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
import time, tempfile

sc = SparkContext()
spark = SparkSession(sc)

with tempfile.TemporaryDirectory() as d:

    # Write a temporary text file to read it.
    spark.createDataFrame(
        [
            {"expense": "meal", "cost": 100, "date": "2024-08-09"},
            {"expense": "travel", "cost": 200, "date": "2024-08-10"},
            {"expense": "training", "cost": 300, "date": "2024-08-11"},
            {"expense": "books", "cost": 400, "date": "2024-08-12"},
        ]
    ).write.mode("overwrite").format("csv").save(d)

    # Start a streaming query to read the CSV file.
    q = (
        spark.readStream.schema("cost INT, expense STRING, date STRING")
        .format("csv")
        .load(d)
        .writeStream.format("console")
        .start()
    )
    time.sleep(3)
    q.stop()
