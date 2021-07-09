HASH_NUM=64
KAFKA_DIR="/Users/rqzrqh/Public/kafka_2.12-2.8.0/bin/"

for i in `seq $HASH_NUM`
do
        INDEX=`expr $i - 1`
        $KAFKA_DIR/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic settle.$INDEX
done

for i in `seq $HASH_NUM`
do
        INDEX=`expr $i - 1`
        $KAFKA_DIR/kafka-topics.sh --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic settle.$INDEX
done
