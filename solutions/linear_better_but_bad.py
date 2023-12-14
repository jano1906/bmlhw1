
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql import functions as F

def spark_linear(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark linear")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_edge = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    df_best = spark.createDataFrame(df_edge.rdd)
    df_last = spark.createDataFrame(df_edge.rdd)
    df_edge = spark.createDataFrame(df_edge.rdd)

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
        #^o tym momencie
        #najkrotsze sciezki dlugosci n i n+1
        # A->B df_last - ścieżki długości N, df_next - ścieżki długości n+1
        # df_last df_next
        #   5       7
        #   6       6
        #   7       5
        df_next = df_next.union(df_last).groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        df_next = spark.createDataFrame(df_next.rdd, schema=df_best.schema)
        df_next_new = df_next.exceptAll(df_last) #te ktore sie skrocily
        df_best_new = df_next.intersect(df_last) #te ktore sie nie skrocily
        df_last = df_next
        # Update the main DataFrame
        df_best = df_best.union(df_best_new)
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        df_best = spark.createDataFrame(df_best.rdd)

        df_next = df_next_new
        df_next = spark.createDataFrame(df_next.rdd, schema=df_best.schema)

        print(cur_paths, sum_cur_length)
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