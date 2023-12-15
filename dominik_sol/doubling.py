import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil


def solve_doubling(inf, outf):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Doubling Solution")
        .config("spark.driver.memory", "3g")
        .config("spark.worker.memory", "2g")
        .getOrCreate()
    )

    shutil.os.mkdir("checkpoints")
    spark.sparkContext.setCheckpointDir("checkpoints")

    best_paths_df = spark.read.csv(inf, header=True, inferSchema=True)

    # Save only best edge A->B
    best_paths_df = best_paths_df.groupBy(["edge_1", "edge_2"]).agg(
        F.min("length").alias("length")
    )

    new_paths_caches = [best_paths_df.persist()]
    new_paths_caches = [new_paths_caches[-1].checkpoint()]

    cur_paths, cur_lengths = -1, -1
    iter_cnt = 0
    
    while True:
        iter_cnt += 1
        next_best_paths = (
            new_paths_caches[-1]
            .alias("st")
            .join(
                new_paths_caches[-1].alias("nd"),
                F.col("st.edge_2") == F.col("nd.edge_1"),
                "inner",
            )
            .select(
                F.col("st.edge_1").alias("edge_1"),
                F.col("nd.edge_2").alias("edge_2"),
                (F.col("st.length") + F.col("nd.length")).alias("length"),
            )
        )
        new_best_paths = (
            next_best_paths.union(new_paths_caches[-1])
            .groupBy(["edge_1", "edge_2"])
            .agg(F.min("length").alias("length"))
        )
        new_paths_caches.append(new_best_paths.persist())

        checkpoints_to_remove = []
        for rdd_dir in os.listdir("checkpoints"):
            for chckpt in os.listdir(f"checkpoints/{rdd_dir}"):
                checkpoints_to_remove.append(
                    os.path.join("checkpoints", rdd_dir, chckpt)
                )

        new_paths_caches.append(new_paths_caches[-1].checkpoint())

        [cache.unpersist() for cache in new_paths_caches[:-1]]
        new_paths_caches = [new_paths_caches[-1]]

        paths = new_paths_caches[-1].count()
        lengths = (
            new_paths_caches[-1]
            .agg(F.sum("length").alias("sum_length"))
            .first()["sum_length"]
        )

        if (paths, lengths) == (cur_paths, cur_lengths):
            break
        cur_paths, cur_lengths = paths, lengths

        for checkpoint in checkpoints_to_remove:
            shutil.rmtree(checkpoint)

    new_paths_caches[-1].toPandas().to_csv(outf, header=True, index=False)
    spark.stop()
    shutil.rmtree("checkpoints")
