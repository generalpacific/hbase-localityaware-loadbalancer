# Check if Hbase home is passed
if [[ $# -ne 1 ]]
then
	echo "Usage ./rebuild.sh <hbase_home>"
	exit
fi

HBASE_DIR=$1

echo "Stoppping HBase"
$HBASE_DIR/bin/stop-hbase.sh
echo "DONE Stoppping HBase"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Copying"
cp LocalityAwareLoadBalancer.java $HBASE_DIR/hbase-server/src/main/java/org/apache/hadoop/hbase/master/balancer/.
if [ $? -ne 0 ]
then
	echo "Error copying"
	exit
fi
echo "DONE Copying"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Building HBase"
cd $HBASE_DIR
mvn package -DskipTests
if [ $? -ne 0 ]
then
	echo "Error building"
	exit
fi
echo "DONE Building HBase"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Starting HBase"
bin/start-hbase.sh
echo "DONE Starting HBase"

