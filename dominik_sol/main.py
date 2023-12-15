import argparse
from linear import solve_linear
from doubling import solve_doubling


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("alg", choices=["linear", "doubling"])
    parser.add_argument("inf", type=str)
    parser.add_argument("outf", type=str)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    if args.alg == "linear":
        solve_linear(args.inf, args.outf)
    elif args.alg == "doubling":
        solve_doubling(args.inf, args.outf)