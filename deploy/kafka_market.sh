MARKET="eth_btc"
KAFKA_DIR="/Users/rqzrqh/Public/kafka_2.12-2.8.0/bin/"

$KAFKA_DIR/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic offer.$MARKET
$KAFKA_DIR/kafka-topics.sh --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic offer.$MARKET

$KAFKA_DIR/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic quote_deals.$MARKET
$KAFKA_DIR//kafka-topics.sh --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic quote_deals.$MARKET

