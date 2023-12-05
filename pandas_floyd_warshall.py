import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("algorithm")
parser.add_argument("input", type=str)
parser.add_argument("output", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    df = pd.read_csv(args.input)
    assert args.algorithm == "doubling"
    assert args.output.endswith(".csv")
    assert list(df.columns) == ["edge_1", "edge_2", "length"]

    # remove self-loops
    loops = df["edge_1"] == df["edge_2"]
    df_no_loops = df[~loops]

    # aggregate min edge values
    df_good = df_no_loops.groupby(["edge_1", "edge_2"])["length"].min().reset_index()
    n_bound = max(max(df["edge_1"]), max(df["edge_2"])) + 1

    for k in range(n_bound):
        filtered_df_good = df_good[df_good["edge_1"] == k]
        joined_df = df_good.merge(
            filtered_df_good, left_on="edge_2", right_on="edge_1", suffixes=("", "_y")
        )
        joined_df["length"] += joined_df["length_y"]
        joined_df["edge_2"] = joined_df["edge_2_y"]
        joined_df = joined_df.drop(
            [col for col in joined_df.columns if col.endswith("_y")], axis=1
        )
        df_good = pd.concat([df_good, joined_df])
        df_good = df_good.groupby(["edge_1", "edge_2"])["length"].min().reset_index()
    
    loops = df_good["edge_1"] == df_good["edge_2"]
    df_no_loops = df_good[~loops]

    df_no_loops.to_csv(args.output, index=False)
