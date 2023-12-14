#!/bin/bash

cd tests
rm -rf _checkpoints
python run_tests.py --prog ../solutions/spark_solution.py --in_dir inputs --out_dir outputs --no_doubling
