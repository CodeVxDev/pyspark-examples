from pyspark.context import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4, 5], 5)
print(rdd.glom().collect())
