#!/bin/bash

gcc -O2 -fPIC -shared -g udf.c -o libcudf.so
