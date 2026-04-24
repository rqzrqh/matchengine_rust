#!/bin/python

from kafka import KafkaProducer
import order_ops

extern_oper_id = 0

user_id = 0
order_id = 100

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
seq = order_ops.get_input_sequence_id() + 1
order_ops.cancel_order(producer, extern_oper_id, user_id, order_id, seq)
