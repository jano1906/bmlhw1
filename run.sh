#!/bin/bash

cd tests
rm -rf _checkpoints
python run_tests.py --prog ../solutions/jo418361_solution.py --in_dir inputs --out_dir outputs --no_doubling
