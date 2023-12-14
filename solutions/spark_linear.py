from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, struct
from pyspark.sql import functions as F
import os
import shutil


def spark_linear(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    dfs_best_queue = []
    dfs_next_queue = []
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark linear")
        .config("spark.executor.memory", "32g")
        .config("spark.driver.memory", "32g")
        .getOrCreate()
    )
    os.mkdir("_checkpoints")
    spark.sparkContext.setCheckpointDir("_checkpoints")
    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_edge = (
        df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length")).cache()
    )
    df_best_temp = (
        df_edge.withColumn("temp", F.struct("length", lit(1)))
        .drop("length")
        .withColumnRenamed("temp", "length")
        .cache()
    )
    dfs_best_queue.append(df_best_temp.checkpoint())
    dfs_next_queue.append(df_best_temp.checkpoint())

    iter = 0
    while True:
        iter += 1
        df_next = (
            dfs_next_queue[-1]
            .alias("a")
            .join(
                df_edge.alias("b"),
                F.col("a.edge_2") == F.col("b.edge_1"),
                "inner",
            )
            .select(
                F.col("a.edge_1").alias("edge_1"),
                F.col("b.edge_2").alias("edge_2"),
                F.struct(
                    (F.col("a.length.length") + F.col("b.length")).alias("length"),
                    (F.col("a.length.col2") + 1).alias("col2"),
                ).alias("length"),
            )
        )

        rm_checkpoints = []
        if iter % 6 == 0:
            for tmp in os.listdir("_checkpoints"):
                for dir in os.listdir(os.path.join("_checkpoints", tmp)):
                    rm_checkpoints.append(os.path.join("_checkpoints", tmp, dir))

        # Update the main DataFrame
        df_best = dfs_best_queue[-1].union(df_next)
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length"),
        )

        dfs_best_queue.append(df_best.cache())
        if iter % 3 == 0:
            dfs_best_queue.append(dfs_best_queue[-1].checkpoint())
            for i in range(len(dfs_best_queue) - 1):
                dfs_best_queue[i].unpersist()
            dfs_best_queue = [dfs_best_queue[-1]]

        df_next = df_next.intersect(df_best)

        dfs_next_queue.append(df_next.cache())
        if iter % 3 == 0:
            dfs_next_queue.append(dfs_next_queue[-1].checkpoint())
            for i in range(len(dfs_next_queue) - 1):
                dfs_next_queue[i].unpersist()
            dfs_next_queue = [dfs_next_queue[-1]]

        for ckpt in rm_checkpoints:
            shutil.rmtree(ckpt)

        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        if df_next.isEmpty():
            break

    # Write result to a single CSV file
    dfs_best_queue[-1].select(["edge_1", "edge_2", "length.length"]).toPandas().to_csv(
        output, header=True, index=False
    )
    spark.stop()
    shutil.rmtree("_checkpoints")
