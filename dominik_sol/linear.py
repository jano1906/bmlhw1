import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil


def solve_linear(inf, outf):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Linear Solution")
        .config("spark.driver.memory", "3g")
        .config("spark.worker.memory", "2g")
        .getOrCreate()
    )
    os.mkdir("checkpoints")
    spark.sparkContext.setCheckpointDir("checkpoints")
    df = spark.read.csv(inf, header=True, inferSchema=True)

    # Aggregate min edge values
    df_edge = (
        df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length")).persist()
    )
    best_paths_df = df_edge.withColumn(
        "length_pair", F.struct("length", F.lit(1))
    ).drop("length")

    new_paths_caches = []
    new_paths_caches.append(best_paths_df.cache())
    new_paths_caches.append(new_paths_caches[-1].checkpoint())

    total_best_paths = new_paths_caches[-1].checkpoint()

    iter_cnt = 0

    while True:
        iter_cnt += 1
        next_paths_df = (
            new_paths_caches[-1]
            .alias("st")
            .join(
                df_edge.alias("nd"),
                F.col("st.edge_2") == F.col("nd.edge_1"),
                "inner",
            )
            .select(
                F.col("st.edge_1").alias("edge_1"),
                F.col("nd.edge_2").alias("edge_2"),
                F.struct(
                    (F.col("st.length_pair.length") + F.col("nd.length")).alias(
                        "length"
                    ),
                    (F.col("st.length_pair.col2") + 1).alias("col2"),
                ).alias("length_pair"),
            ).persist()
        )

        checkpoints_to_remove = []
        for rdd_dir in os.listdir("checkpoints"):
            for chckpt in os.listdir(f"checkpoints/{rdd_dir}"):
                checkpoints_to_remove.append(
                    os.path.join("checkpoints", rdd_dir, chckpt)
                )

        total_best_paths = (
            total_best_paths.union(next_paths_df)
            .groupBy(["edge_1", "edge_2"])
            .agg(
                F.min("length_pair").alias("length_pair"),
            )
        ).checkpoint()

        [cache.unpersist() for cache in new_paths_caches[:-1]]
        new_paths_caches = [new_paths_caches[-1]]

        df_new_edges = next_paths_df.intersect(total_best_paths)

        new_paths_caches.append(df_new_edges.checkpoint())

        if df_new_edges.isEmpty():
            break
        
        next_paths_df.unpersist()

        for checkpoint in checkpoints_to_remove:
            shutil.rmtree(checkpoint)

    # Write result to a single CSV file
    total_best_paths.select(
        ["edge_1", "edge_2", "length_pair.length"]
    ).withColumnRenamed("length_pair.length", "length").toPandas().to_csv(
        outf, header=True, index=False
    )
    spark.stop()
    shutil.rmtree("checkpoints")
