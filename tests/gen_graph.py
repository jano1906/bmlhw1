import numpy as np
import argparse
import pandas as pd

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=100)
    parser.add_argument("--prob", type=float, default = 0.25)
    parser.add_argument("--max_w", type=int, default=1)
    parser.add_argument("--n_graphs", type=int, default=1)
    parser.add_argument("--output", type=str, required=True)
    return parser

def gen_graph(nodes, prob, max_w, n_graphs):
    def _gen_graph():
        xs, ys = np.meshgrid(np.arange(nodes), np.arange(nodes))
        ws = np.random.randint(1, max_w+1, size=xs.shape)
        ps = np.random.binomial(1, prob, size=xs.shape)
        ps = ps.flatten()
        ws = ws.flatten()[ps == 0]
        xs = xs.flatten()[ps == 0]
        ys = ys.flatten()[ps == 0]
        df = pd.DataFrame({
            "edge_1": xs,
            "edge_2": ys,
            "length": ws,
        })
        return df
    dfs = [_gen_graph() for _ in range(n_graphs)]
    df = pd.concat(dfs)
    return df

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    assert args.output.endswith(".csv")
    df = gen_graph(args.nodes, args.prob, args.max_w, args.n_graphs)
    df.to_csv(args.output, sep=",", index=False)