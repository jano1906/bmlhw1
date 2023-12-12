import argparse
from spark_doubling import spark_doubling
from spark_linear import spark_linear
import spark_doubling_old
import spark_linear_old

parser = argparse.ArgumentParser()
parser.add_argument("algorithm", choices=["linear", "doubling"])
parser.add_argument("input", type=str)
parser.add_argument("output", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    assert args.output.endswith(".csv"), f"got input file {args.output}"

    if args.algorithm == "linear":
#        spark_linear_old.spark_linear(args.input, args.output)
        spark_linear(args.input, args.output)
    elif args.algorithm == "doubling":
        spark_doubling_old.spark_doubling(args.input, args.output)
#        spark_doubling(args.input, args.output)