import argparse
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, default="example_graph.csv")
parser.add_argument("--output", type=str)

if __name__ == '__main__':
    args = parser.parse_args()
    df = pd.read_csv(args.input)
    assert args.output.endswith(".csv")
    assert list(df.columns) == ['edge_1', 'edge_2', 'length']
    n = max(max(df['edge_1']), max(df['edge_2']))
    max_len = max(df['length'])
    n_bound = n + 1
    INF = max_len * n_bound + 1
    dist = [[INF for _ in range(n_bound)] for _ in range(n_bound)]
    #reading shortest edges from dataframe
    for _, row in df.iterrows():
        f, t, v = row['edge_1'], row['edge_2'], row['length']
        dist[f][t] = min(dist[f][t], v)
    
    #removing self loops
    for x in range(n_bound):
        dist[x][x] = 0
    
    for k in range(n_bound):
        for i in range(n_bound):
            for j in range(n_bound):
                dp[i][j] = min(dp[i][j], dp[i][k] + dp[k][j])
    result = pd.DataFrame(columns=['edge_1', 'edge_2', 'length'])
    for i in range(n_bound):
        for j in range(n_bound):
            if i == j or dp[i][j] == INF:
                continue
            result.loc[len(result)] = [i, j, dp[i][j]]
    result.to_csv(args.output, index=False)