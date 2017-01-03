#!/usr/bin/env bash

export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$PYTHONPATH

python -m unittest discover