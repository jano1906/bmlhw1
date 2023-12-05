import numpy as np
import argparse
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--nodes", type=int, default=100)
parser.add_argument("--prob", type=float, default = 0.25)
parser.add_argument("--max_w", type=int, default=1)
parser.add_argument("--out", type=str, required=True)

if __name__ == "__main__":
    args = parser.parse_args()
    n = args.nodes
    xs, ys = np.meshgrid(np.arange(n), np.arange(n))
    ws = np.random.randint(1, args.max_w+1, size=xs.shape)
    ps = np.random.binomial(1, args.prob, size=xs.shape)
    ps = ps.flatten()
    ws = ws.flatten()[ps == 0]
    xs = xs.flatten()[ps == 0]
    ys = ys.flatten()[ps == 0]

    df = pd.DataFrame({
        "edge_1": xs,
        "edge_2": ys,
        "length": ws,
    })
    df.to_csv(args.out + ".csv", sep=",", index=False)