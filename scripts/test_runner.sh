#!/bin/bash
export CLASSPATH=$(hadoop classpath --glob)
echo "CLASSPATH set to: $CLASSPATH"
/opt/python3.9.12/bin/python3 /tmp/5g-training/test_hdfs.py
