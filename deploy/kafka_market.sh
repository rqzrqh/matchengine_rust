if [ $# -ne 1 ];then
	echo "args error"
	exit 1
fi

MARKET=$1

kafka-topics --bootstrap-server localhost:9092 --delete --topic offer.$MARKET
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic offer.$MARKET

kafka-topics --bootstrap-server localhost:9092 --delete --topic quote_deals.$MARKET
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic quote_deals.$MARKET

