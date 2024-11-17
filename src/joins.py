from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

sc = SparkContext()
spark = SparkSession(sc)
df_expenses: DataFrame = spark.createDataFrame(
    [
        {"id": 1, "expense": "meal", "cost": 100, "date": "2024-08-09", "paid": True},
        {"id": 2, "expense": "travel", "cost": 200, "date": "2024-08-10"},
        {
            "id": 3,
            "expense": "training",
            "cost": 300,
            "date": "2024-08-11",
            "paid": True,
        },
        {"id": 4, "expense": "books", "cost": 400, "date": "2024-08-12"},
        {"id": 5, "expense": "meal", "cost": 300, "date": "2024-08-13", "paid": True},
        {"id": 6, "expense": "travel", "cost": 600, "date": "2024-08-14"},
        {"id": 7, "expense": "travel", "cost": 200, "date": "2024-08-15", "paid": True},
        {"id": 8, "expense": "training", "cost": 100, "date": "2024-08-16"},
    ]
)

df_approvals: DataFrame = spark.createDataFrame(
    [
        {"id": 1, "status": "approved", "approval_date": "2024-08-09", "expense_id": 1},
        {"id": 2, "status": "approved", "approval_date": "2024-08-11", "expense_id": 3},
        {"id": 3, "status": "approved", "approval_date": "2024-08-13", "expense_id": 5},
        {"id": 4, "status": "approved", "approval_date": "2024-08-15", "expense_id": 7},
        {"id": 5, "status": "approved", "approval_date": "2024-08-16", "expense_id": 8},
        {"id": 6, "status": "approved", "approval_date": "2024-08-17", "expense_id": 9},
        {
            "id": 7,
            "status": "approved",
            "approval_date": "2024-08-18",
            "expense_id": 10,
        },
        {
            "id": 8,
            "status": "approved",
            "approval_date": "2024-08-19",
            "expense_id": 11,
        },
    ]
)

# df_expenses.join(df_approvals, on="id", how="cross").show()
df_expenses.join(
    df_approvals, how="anti", on=df_expenses.id == df_approvals.expense_id
).show()

spark.sql(
    "SELECT * FROM {expenses} e1 ANTI JOIN {approvals} a1 ON e1.id = a1.expense_id",
    expenses=df_expenses,
    approvals=df_approvals,
).show()
