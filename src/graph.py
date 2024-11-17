from graphframes import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

sc = SparkContext()
spark = SparkSession(sc)
df_expenses: DataFrame = spark.createDataFrame(
    [
        {"id": 12, "name": "Alice", "jdate": "2024-08-09"},
        {"id": 16, "name": "Bob", "jdate": "2024-08-10"},
        {"id": 11, "name": "Mark", "jdate": "2024-08-11"},
        {"id": 14, "name": "John", "jdate": "2024-08-12"},
        {"id": 17, "name": "Rob", "jdate": "2024-08-13"},
    ]
)

df_approvals: DataFrame = spark.createDataFrame(
    [
        {"src": 12, "dst": 16, "relationship": "Manager"},
        {"src": 16, "dst": 11, "relationship": "Peer"},
        {"src": 12, "dst": 14, "relationship": "Peer"},
        {"src": 14, "dst": 17, "relationship": "Manager"},
        {"src": 14, "dst": 11, "relationship": "Manager"},
        {"src": 11, "dst": 17, "relationship": "Supervisor"},
        {"src": 17, "dst": 12, "relationship": "Supervisor"},
    ]
)

g = GraphFrame(df_expenses, df_approvals)

g.inDegrees.show()

# Query: Count the number of "Manager" connections in the graph.
print(g.edges.filter("relationship = 'Manager'").count())

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
