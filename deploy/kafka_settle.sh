HASH_NUM=64

kafka-topics --bootstrap-server localhost:9092 --delete --topic settle
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions $HASH_NUM --topic settle
kafka-topics --bootstrap-server localhost:9092 --describe --topic settle