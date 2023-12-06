import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("algorithm")
parser.add_argument("input", type=str)
parser.add_argument("output", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    df = pd.read_csv(args.input)
    assert args.algorithm == "linear"
    assert args.output.endswith(".csv"), f"got {args.output}"
    assert list(df.columns) == ["edge_1", "edge_2", "length"], f"got {df.columns}"

    # remove self-loops
    df_no_loops = df[df["edge_1"] != df["edge_2"]]

    # aggregate min edge values
    df_good = df_no_loops.groupby(["edge_1", "edge_2"])["length"].min().reset_index()
    n_bound = max(max(df["edge_1"]), max(df["edge_2"])) + 1
    last_len, last_sum = 0, 0
    while (last_len, last_sum) != (len(df_good), sum(df_good['length'])):
        last_len, last_sum = len(df_good), sum(df_good['length'])
        joined_df = df_good.merge(
            df_good, left_on="edge_2", right_on="edge_1", suffixes=("", "_y")
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
