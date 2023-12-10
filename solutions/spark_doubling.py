from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql import functions as F


def spark_doubling(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark floyd warshall")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir("./checkpoints")
    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_good = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    df_good = df_good.localCheckpoint()

    cur_paths, sum_cur_length = -1, -1
    iter = 0
    while True:
        iter += 1
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
        df_good = df_good.union(joined_df)
        df_good = df_good.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        df_good.unpersist()
        df_good = df_good.localCheckpoint()

        print(cur_paths, sum_cur_length)
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        next_cur_paths = df_good.count()
        next_sum_cur_length = df_good.select(
            sum("length").alias("sum_length")
        ).collect()[0]["sum_length"]

        if (next_cur_paths, next_sum_cur_length) == (cur_paths, sum_cur_length):
            break
            
        cur_paths, sum_cur_length = next_cur_paths, next_sum_cur_length

    # Write result to a single CSV file
    df_good.toPandas().to_csv(output, header=True, index=False)
