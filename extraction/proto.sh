#!/bin/bash

SRC_DIR=src/main/proto
DST_DIR=src/main/java

protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/ParsedPage.proto