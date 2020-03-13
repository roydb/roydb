#! /bin/bash

go build -o libudf.so --buildmode=c-shared udf.go
