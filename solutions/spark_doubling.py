from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least
from pyspark.sql import functions as F


def get_max_from_col(df, column):
    return df.agg(F.max(col(column)).alias("max_column")).first()["max_column"]


def spark_doubling(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark floyd warshall")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir('./checkpoints')
    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_good = (
        df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    )
    df_good.localCheckpoint()

    # Get the upper bound for the iteration
    n_bound = (
        max(
            df.select(F.max("edge_1")).collect()[0][0],
            df.select(F.max("edge_2")).collect()[0][0],
        )
        + 1
    )

    for k in range(n_bound):
        joined_df = (
            df_good.alias("a")
            .join(
                df_good.alias("b"),
                F.col("a.edge_2") == F.col("b.edge_1"),
                "inner",
            )
            .select(
                F.col("a.edge_1").alias("edge_1"),
                F.col("b.edge_2").alias("edge_2"),
                (F.col("a.length") + F.col("b.length")).alias("length"),
            )
        )
        # Update the main DataFrame
        df_good2 = df_good.union(joined_df)
        df_good3 = (
            df_good2.groupBy(["edge_1", "edge_2"])
            .agg(F.min("length").alias("length"))
        )
        #df_good2 = df_good.alias("a").join(
        #    joined_df.alias("b"),
        #    (F.col("a.edge_1") == F.col("b.edge_1")) & (F.col("a.edge_2") == F.col("b.edge_2")),
        #    "outer"
        #).select(
        #    F.col("a.edge_1").alias("edge_1"),
        #    F.col("b.edge_2").alias("edge_2"),
        #    (least(F.col("a.length"), F.col("b.length"))).alias("length")
        #)
        df_good = df_good3
        df_good = df_good.cache()

        if k % 5 == 0:
            df_good = df_good.localCheckpoint()
        
        print(k)

    # Write result to a single CSV file
    df_good.toPandas().to_csv(output, header=True, index=False)
