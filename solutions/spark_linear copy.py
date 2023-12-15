
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
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
    os.mkdir('_checkpoints')
    spark.sparkContext.setCheckpointDir('_checkpoints')
    df = spark.read.csv(input, header=True, inferSchema=True)
    
    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_edge = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    df_edge = df_edge.cache()
    dfs_best_queue.append(df_edge.checkpoint())
    dfs_next_queue.append(df_edge.checkpoint())
    
    cur_paths, sum_cur_length = -1, -1
    iter = 0
    while True:
        iter += 1
        df_next = (
            dfs_next_queue[-1].alias("a")
            .join(
                df_edge.alias("b"),
                F.col("a.edge_2") == F.col("b.edge_1"),
                "inner",
            )
            .select(
                F.col("a.edge_1").alias("edge_1"),
                F.col("b.edge_2").alias("edge_2"),
                (F.col("a.length") + F.col("b.length")).alias("length"),
            )
        )
        df_next = df_next.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        #df_next = df_next.exceptAll(df_best)
        
        rm_checkpoints = []
        if iter % 6 == 0:
            for tmp in os.listdir("_checkpoints"):
                for dir in os.listdir(os.path.join("_checkpoints", tmp)):
                    rm_checkpoints.append(os.path.join("_checkpoints", tmp, dir))


        dfs_next_queue.append(df_next.cache())
        if iter % 3 == 0:
            dfs_next_queue.append(dfs_next_queue[-1].checkpoint())
            for i in range(len(dfs_next_queue)-1):
                dfs_next_queue[i].unpersist()
            dfs_next_queue = [dfs_next_queue[-1]]
        
        # Update the main DataFrame
        df_best = dfs_best_queue[-1].union(dfs_next_queue[-1])
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )

        dfs_best_queue.append(df_best.cache())
        if iter % 3 == 0:
            dfs_best_queue.append(dfs_best_queue[-1].checkpoint())
            for i in range(len(dfs_best_queue)-1):
                dfs_best_queue[i].unpersist()
            dfs_best_queue = [dfs_best_queue[-1]]
        
        for ckpt in rm_checkpoints:
            shutil.rmtree(ckpt)
        
        print(f"[debug] cur_paths: {cur_paths}, sum cur length: {sum_cur_length}")
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        next_cur_paths = dfs_best_queue[-1].count()
        next_sum_cur_length = dfs_best_queue[-1].select(
            sum("length").alias("sum_length")
        ).collect()[0]["sum_length"]

        if (next_cur_paths, next_sum_cur_length) == (cur_paths, sum_cur_length):
            break
            
        cur_paths, sum_cur_length = next_cur_paths, next_sum_cur_length

    # Write result to a single CSV file
    dfs_best_queue[-1].toPandas().to_csv(output, header=True, index=False)
    spark.stop()
    shutil.rmtree("_checkpoints")