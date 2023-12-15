#!/bin/bash

cd tests
#rm -rf checkpoints
python run_tests.py --prog ../solutions/jo418361_solution.py --in_dir krz_input --out_dir krz_output --no_doubling
