#! /bin/sh

cd $(dirname $0)
CLASSPATH=''
for I in $(ls lib); do
    CLASSPATH=$CLASSPATH:./lib/$I
done

java -cp .:./dist/lib/hbase_compact.jar:$CLASSPATH com.stumbleupon.hbaseadmin.HBaseCompact $*
#java -cp .:./dist/lib/hbase_compact.jar:$CLASSPATH com.stumbleupon.hbaseadmin.HBaseRegionMover $*
