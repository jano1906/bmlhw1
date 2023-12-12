#!/bin/bash

cd tests
rm -rf _checkpoints
python run_tests.py --prog ../solutions/spark_solution.py --in_dir krz_input --out_dir krz_output --no_doubling # > explain.txt
