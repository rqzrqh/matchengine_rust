HASH_NUM=64

for i in `seq $HASH_NUM`
do
        INDEX=`expr $i - 1`
        kafka-topics --bootstrap-server localhost:9092 --delete --topic settle.$INDEX
done

for i in `seq $HASH_NUM`
do
        INDEX=`expr $i - 1`
        kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic settle.$INDEX
done
