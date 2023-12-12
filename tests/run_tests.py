import argparse
import tempfile
import subprocess
import os
import pandas as pd
import time
import shutil

class style():
  RED = '\033[31m'
  GREEN = '\033[32m'
  BLUE = '\033[34m'
  RESET = '\033[0m'

def colored_txt(s, c):
    d = {
        "r": style.RED,
        "g": style.GREEN,
        "b": style.BLUE
    }
    return d[c] + str(s) + style.RESET

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prog", type=str, required=True)
    parser.add_argument("--in_dir", type=str, default="inputs")
    parser.add_argument("--out_dir", type=str, default="outputs")
    parser.add_argument("--no_linear", action="store_true")
    parser.add_argument("--no_doubling", action="store_true")
    return parser

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    options = []
    if not args.no_linear:
        options.append("linear")
    if not args.no_doubling:
        options.append("doubling")

    inputs = [os.path.join(args.in_dir, f) for f in os.listdir(args.in_dir) if os.path.splitext(f)[1] == ".csv"]
    outputs = [os.path.join(args.out_dir, f) for f in os.listdir(args.out_dir) if os.path.splitext(f)[1] == ".csv"]
    inputs.sort()
    outputs.sort()

    assert all(os.path.basename(i) == os.path.basename(o) for i, o in zip(inputs, outputs))
    os.makedirs("logs", exist_ok=True)
    with open(os.path.join("logs", "times.txt"), "a") as f:
        f.write("========================= \n")
    rets = []
    for in_file, out_file in zip(inputs, outputs):
        for option in options:
            with tempfile.NamedTemporaryFile(suffix=".csv") as fp:
                start = time.time()
                subprocess.run(["python3", os.path.abspath(args.prog), option, in_file, fp.name], check=True)
                end = time.time()
                prog_out = pd.read_csv(fp)
                test_out = pd.read_csv(out_file)
                prog_out = prog_out.sort_values(["edge_1", "edge_2"]).reset_index(drop=True)
                test_out = test_out.sort_values(["edge_1", "edge_2"]).reset_index(drop=True)
                ret = prog_out.equals(test_out)
                rets.append(ret)
                color = "g" if ret else "r"
                comm = "OK" if ret else "ERROR"
                comm += " " + os.path.basename(in_file) + f"time = {end - start}"
                with open(os.path.join("logs", "times.txt"), "a") as f:
                    f.write(f"{os.path.basename(in_file)}, {end - start}\n")
                print(colored_txt(comm, color))
                if not ret:
                    out = pd.concat([prog_out, test_out], axis=1)
                    prog_columns = ["prog_" + c for c in prog_out.columns]
                    test_columns = ["test_" + c for c in test_out.columns]
                    out.columns = prog_columns + test_columns
                    print(out[out.apply(lambda r: any(r[prog_columns].values != r[test_columns].values), axis=1)])

    print(f"tests passed: {sum(rets)} / {len(rets)}")