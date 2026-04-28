HASH_NUM=64

# Remove legacy per-shard topics (settle.0 .. settle.63) if present.
for i in `seq $HASH_NUM`
do
        INDEX=`expr $i - 1`
        kafka-topics --bootstrap-server localhost:9092 --delete --topic settle.$INDEX
done

kafka-topics --bootstrap-server localhost:9092 --delete --topic settle

kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions $HASH_NUM --topic settle
