CACHE_DIR="/tmp/tsdb-sql"
rm -rf $CACHE_DIR/*

TSDB_DIR=.
mkdir -p $TSDB_DIR/log

DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8003"

export JAVA="java"
export JVMARGS="-enableassertions -enablesystemassertions -Xmx3000m $DEBUG"

set -x
nohup $TSDB_DIR/build/tsdb tsd \
 --port=14242 \
 --staticroot=$TSDB_DIR/build/staticroot \
 --cachedir="$CACHE_DIR" \
 --auto-metric \
 --dbhost 192.168.56.135 \
 --dbuser root \
 --dbpass "root123" \
 --dbname trends_tsdb \
>> $TSDB_DIR/log/opentsdb-sql.log 2>&1 &
