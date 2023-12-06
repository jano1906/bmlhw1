import os
import numpy as np
import argparse

import gen_graph
import floyd_warshall

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_dir", type=str, default="inputs")
    parser.add_argument("--out_dir", type=str, default="outputs")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--n_tests", type=int, default=5)
    return parser

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    os.makedirs(args.in_dir, exist_ok=args.force)
    os.makedirs(args.out_dir, exist_ok=args.force)
    NODES = np.random.randint(10, 100, size=[args.n_tests])
    PROB = np.random.uniform(0.1, 0.8, size=[args.n_tests])
    MAX_W = np.random.randint(1, 20, size=[args.n_tests])

    for i in range(args.n_tests):
        df_in = gen_graph.gen_graph(NODES[i], PROB[i], MAX_W[i])
        df_in.to_csv(os.path.join(args.in_dir, f"graph_{i}.csv"), sep=",", index=False)
        df_out = floyd_warshall.floyd_warshall(df_in)
        df_out.to_csv(os.path.join(args.out_dir, f"graph_{i}.csv"), sep=",", index=False)
