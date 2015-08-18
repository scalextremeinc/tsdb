#!/bin/bash


if [ ! -z $HBASE_PORT_2181_TCP_PORT ]; then
    ZKQUORUM=hbase
fi
if [ -z $ZKQUORUM ]; then
    echo "ZKQUORUM is unset"
    exit 1
fi

CACHE_DIR=${CACHE_DIR:-"/tmp/tsdb-nosql"}
TSDB_HOME=${TSDB_HOME:-.}
TSDB_PORT=${TSDB_PORT:-4242}
export JAVA=${JAVA:-"java"}
export JVMARGS=${JVMARGS:-"-enableassertions -enablesystemassertions -Xmx3000m"}

mkdir -p $CACHE_DIR
mkdir -p $LOG_DIR
rm -rf $CACHE_DIR/*

exec $TSDB_HOME/build/tsdb tsd \
 --port=$TSDB_PORT \
 --staticroot=$TSDB_HOME/build/staticroot \
 --cachedir="$CACHE_DIR" \
 --zkquorum $ZKQUORUM \
 --auto-metric
