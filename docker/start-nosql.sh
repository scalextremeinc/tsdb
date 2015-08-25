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
BINDPORT=${BINDPORT:-4242}
export JAVA=${JAVA:-"java"}
export JVMARGS=${JVMARGS:-"-enableassertions -enablesystemassertions -Xmx3000m"}

if [ ! -z "$HOSTS_ENTRY" ]; then
    grep "$HOSTS_ENTRY" /etc/hosts
    if [ $? == 1 ]; then
        # this is needed to resolve dns name reported by hbase
        echo "$HOSTS_ENTRY" >> /etc/hosts
    fi
fi

mkdir -p $CACHE_DIR
mkdir -p $LOG_DIR
rm -rf $CACHE_DIR/*

exec $TSDB_HOME/build/tsdb tsd \
 --port=$BINDPORT \
 --staticroot=$TSDB_HOME/build/staticroot \
 --cachedir="$CACHE_DIR" \
 --zkquorum $ZKQUORUM \
 --auto-metric
