# Project Name

This is a script to assign shards among different nodes in a balanced way by using binary search

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)


## Installation

The source code is written in Python. You will need python3 to run the script.

To set up  virtualenv install python3, then run

`pyenv virtualenv $(cat .python-version) env`; start a new shell; `pyenv activate env`

## Usage

if you are using any version >=3.7, the command to run is

```
./src/assign_shards.py -s data/shards.json -n data/nodes.json -r 1

```

The output will be written in `output`, and also printed out in terminal.

`output/shard_assignment_results.json`: the results of shard assignments

`output/replica_assignment_results.json`: the results of replica of these shards

**Args**

`--shard` or `-s`: the file path to store  info of unassigned shards 

`--nodes` or `-n`: the file path to store info of nodes

`--replica` or `-r`: the number of replica of the primary shard

**Data**

`/data`: the input data

`/output`: the assignment results

**Source Code**

`/src`: the folder where source code are stored

**Tests**

`/tests`: the folder where the tests are stored


## Questions
Sometime you may run into `module src not found` problem, run ` pip install -e .` will likely fix it 