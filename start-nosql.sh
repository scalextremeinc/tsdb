CACHE_DIR="/tmp/tsdb-nosql"
rm -rf $CACHE_DIR/*

TSDB_DIR=.
mkdir -p $TSDB_DIR/log

DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8002"

export JAVA="java"
export JVMARGS="-enableassertions -enablesystemassertions -Xmx3000m $DEBUG"

nohup $TSDB_DIR/build/tsdb tsd \
 --port=4242 \
 --staticroot=$TSDB_DIR/build/staticroot \
 --cachedir="$CACHE_DIR" \
 --zkquorum 127.0.0.1 \
 --auto-metric \
>> $TSDB_DIR/log/opentsdb-nosql.log 2>&1 &
