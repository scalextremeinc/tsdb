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
export JAVA=${JAVA:-"java"}
export JVMARGS=${JVMARGS:-"-enableassertions -enablesystemassertions -Xmx3000m"}

mkdir -p $CACHE_DIR
mkdir -p $LOG_DIR
rm -rf $CACHE_DIR/*

nohup $TSDB_HOME/build/tsdb tsd \
 --port=4242 \
 --staticroot=$TSDB_HOME/build/staticroot \
 --cachedir="$CACHE_DIR" \
 --zkquorum $ZKQUORUM \
 --auto-metric \
>> $LOG_DIR/opentsdb-nosql.log 2>&1 &
tail -F $LOG_DIR/opentsdb-nosql.log
