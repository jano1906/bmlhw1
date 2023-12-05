import argparse
import tempfile
import subprocess
import os

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prog", type=str, required=True)
    parser.add_argument("--in_dir", type=str, default="inputs")
    parser.add_argument("--out_dir", type=str, default="outputs")
    parser.add_argument("--no_linear", action="store_true")
    parser.add_argument("--no_doubling", action="store_true")

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    options = []
    if not args.no_linear:
        options.append("linear")
    if not args.no_doubling:
        options.append("doubling")

    inputs = [f for f in os.listdir(args.in_dir) if os.path.splitext(f)[1] == ".csv"]
    outputs = [f for f in os.listdir(args.out_dir) if os.path.splitext(f)[1] == ".csv"]
    inputs.sort()
    outputs.sort()    
    assert all(os.path.basename(i) == os.path.basename(o) for i, o in zip(inputs, outputs))
    
    for in_file, out_file in zip(inputs, outputs):
        for option in options:
            with tempfile.TemporaryFile() as fp:
                subprocess.run([args.prog, option, in_file, fp])
            assert False