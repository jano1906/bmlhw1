import argparse
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil


CHECKPOINT_DIR = "checkpoints"


class MemoryManager:
    def __init__(self, checkpoint_every_n, clear_checkpoints_every_n):
        self.checkpoint_every_n = checkpoint_every_n
        self.clear_checkpoints_every_n = clear_checkpoints_every_n
        self._step = 0
        self._cache = {}
        self.protected_checkpoints = []

    def protect_cur_checkpoints(self):
        for tmp in os.listdir(CHECKPOINT_DIR):
            for dir in os.listdir(os.path.join(CHECKPOINT_DIR, tmp)):
                self.protected_checkpoints.append(os.path.join(CHECKPOINT_DIR, tmp, dir))

    def checkpoint(self):
        for k in self._cache:
            self._cache[k].append(self._cache[k][-1].checkpoint())
            for i in range(len(self._cache[k])-1):
                self._cache[k][i].unpersist()
            self._cache[k] = [self._cache[k][-1]]

    def step(self):
        self._step += 1
        if (self._step % self.checkpoint_every_n) == 0:
            rm_checkpoints = []
            if (self._step % self.clear_checkpoints_every_n) == 0:
                for tmp in os.listdir(CHECKPOINT_DIR):
                    for dir in os.listdir(os.path.join(CHECKPOINT_DIR, tmp)):
                        rm_checkpoints.append(os.path.join(CHECKPOINT_DIR, tmp, dir))
    
            self.checkpoint()
            
            for ckpt in rm_checkpoints:
                if ckpt not in self.protected_checkpoints:
                    shutil.rmtree(ckpt)

    def cache(self, **kwargs):
        for k, v in kwargs.items():
            if k not in self._cache:
                self._cache[k] = []
            self._cache[k].append(v.persist())

    def __getitem__(self, name):
        return self._cache[name][-1]



def solve_doubling(spark, mem, df):
    df_best = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length"))
    mem.cache(df_best=df_best)
    
    cur_paths, sum_cur_length = -1, -1
    iter = 0
    while True:
        iter += 1
        joined_df = (
            mem["df_best"].alias("a")
            .join(
                mem["df_best"].alias("b"),
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
        df_best = mem["df_best"].union(joined_df)
        df_best = df_best.groupBy(["edge_1", "edge_2"]).agg(
            F.min("length").alias("length")
        )
        mem.cache(df_best=df_best)
        mem.step()
        
        print(f"[debug] cur_paths: {cur_paths}, sum cur length: {sum_cur_length}")
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

        next_cur_paths = mem["df_best"].count()
        next_sum_cur_length = mem["df_best"].select(
            F.sum("length").alias("sum_length")
        ).collect()[0]["sum_length"]

        if (next_cur_paths, next_sum_cur_length) == (cur_paths, sum_cur_length):
            break
            
        cur_paths, sum_cur_length = next_cur_paths, next_sum_cur_length

    return mem["df_best"]

def solve_linear(spark, mem, df):
    df_edge = df.groupBy(["edge_1", "edge_2"]).agg(F.min("length").alias("length")).persist().checkpoint()
    mem.protect_cur_checkpoints()

    best_paths = df_edge.withColumn(
        "length_pair", F.struct("length", F.lit(1))
    ).drop("length")

    mem.cache(new_paths=best_paths, best_paths=best_paths)
    mem.checkpoint()

    iter_cnt = 0

    while True:
        iter_cnt += 1
        new_paths = (
            mem["new_paths"]
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
            )
        )
        mem.cache(new_paths=new_paths)

        best_paths = (
            mem["best_paths"].union(new_paths)
            .groupBy(["edge_1", "edge_2"])
            .agg(
                F.min("length_pair").alias("length_pair"),
            )
        )
        mem.cache(best_paths=best_paths)


        improved_paths = mem["new_paths"].intersect(mem["best_paths"])
        mem.cache(new_paths=improved_paths)

        if mem["new_paths"].isEmpty():
            break
        
        mem.step()
        
        print(
            f"[debug] {iter}, len of caches dfs {len(spark.sparkContext._jsc.getPersistentRDDs().items())}"
        )

    # Write result to a single CSV file
    return mem["best_paths"].select(
        ["edge_1", "edge_2", "length_pair.length"]
    ).withColumnRenamed("length_pair.length", "length")


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("algorithm", choices=["linear", "doubling"])
    parser.add_argument("input", type=str)
    parser.add_argument("output", type=str)
    return parser

def main(args):

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Spark doubling")
        .config("spark.executor.memory", "32g")
        .config("spark.driver.memory", "32g")
        .getOrCreate()
    )
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)
    df = spark.read.csv(args.input, header=True, inferSchema=True)
    assert df.columns == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    mem = MemoryManager(2, 1)
    if args.algorithm == "linear":
        ret = solve_linear(spark, mem, df)
    elif args.algorithm == "doubling":
        ret = solve_doubling(spark, mem, df)
    else:
        assert False

    # Write result to a single CSV file
    ret.toPandas().to_csv(args.output, header=True, index=False)
    spark.stop()

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    assert args.output.endswith(".csv"), f"got input file {args.output}"
    main(args)