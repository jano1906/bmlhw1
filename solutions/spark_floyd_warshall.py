from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def get_max_from_col(df, column):
    return df.agg(F.max(col(column)).alias("max_column")).first()["max_column"]


def spark_floyd_warshall(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark floyd warshall")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_good = (
        df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length")).cache()
    )

    # Get the upper bound for the iteration
    n_bound = (
        max(
            df.select(F.max("edge_1")).collect()[0][0],
            df.select(F.max("edge_2")).collect()[0][0],
        )
        + 1
    )

    for k in range(n_bound):
        print(k)
        # Filter DataFrame for a specific value of edge_1
        filtered_df_good = df_good.filter(df_good["edge_1"] == k)

        # filtered_df_good.show()

        joined_df = (
            df_good.alias("a")
            .join(
                filtered_df_good.alias("b"),
                F.col("a.edge_2") == F.col("b.edge_1"),
                "inner",
            )
            .select(
                F.col("a.edge_1").alias("edge_1"),
                F.col("b.edge_2").alias("edge_2"),
                (F.col("a.length") + F.col("b.length")).alias("length"),
            )
            .cache()
        )
        # Update the main DataFrame
        df_good = df_good.union(joined_df).cache()
        df_good = (
            df_good.groupBy(["edge_1", "edge_2"])
            .agg(F.min("length").alias("length"))
            .cache()
        )

    # Write result to a single CSV file
    df_good.toPandas().to_csv(output, header=True, index=False)
