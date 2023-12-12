
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql import functions as F
import os
import shutil

def spark_linear(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark linear")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    os.mkdir('_checkpoints')
    spark.sparkContext.setCheckpointDir('_checkpoints')
    df = spark.read.csv(input, header=True, inferSchema=True)
    
    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_edge = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    df_edge = df_edge.cache()
    df_best = df_edge.checkpoint()
    df_last = df_edge.checkpoint()
    df_edge = df_edge.checkpoint()

    cur_paths, sum_cur_length = -1, -1
    iter = 0
    while True:
        iter += 1
        df_next = (
            df_last.alias("a")
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
        
        df_next = df_next.cache()
        if iter % 5 == 0:
            df_next = df_next.checkpoint()
        
        df_last = df_next
        # Update the main DataFrame
        df_best = df_best.union(df_next)
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        df_best = df_best.cache()
        if iter % 5 == 0:
            df_best = df_best.checkpoint()
        
        print(f"[debug] cur_paths: {cur_paths}, sum cur length: {sum_cur_length}")
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        next_cur_paths = df_best.count()
        next_sum_cur_length = df_best.select(
            sum("length").alias("sum_length")
        ).collect()[0]["sum_length"]

        if (next_cur_paths, next_sum_cur_length) == (cur_paths, sum_cur_length):
            break
            
        cur_paths, sum_cur_length = next_cur_paths, next_sum_cur_length

    # Write result to a single CSV file
    df_best.toPandas().to_csv(output, header=True, index=False)
    spark.stop()
    shutil.rmtree("_checkpoints")