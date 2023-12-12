import argparse
import os
import pandas as pd

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    return parser

def prepare(path):
    df = pd.read_csv(path, sep = " ", header=None)
    df.columns = ["edge_1", "edge_2"]
    df["length"] = 1
    return df

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    output = os.path.splitext(args.input)[0] + ".csv"
    df = prepare(args.input)
    df.to_csv(output, header=True, index=False)   