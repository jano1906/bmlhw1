from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql import functions as F


def spark_doubling(input, output):
    # TODO: spark.executor.memory and spark.driver.memory
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark doubling")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    df = spark.read.csv(input, header=True, inferSchema=True)

    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # Aggregate min edge values
    df_best = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    df_best = spark.createDataFrame(df_best.rdd)

    cur_paths, sum_cur_length = -1, -1
    iter = 0
    while True:
        iter += 1
        joined_df = (
            df_best.alias("a")
            .join(
                df_best.alias("b"),
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
        df_best = df_best.union(joined_df)
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        df_best = spark.createDataFrame(df_best.rdd)

        print(cur_paths, sum_cur_length)
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        df_best.printSchema()

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
