#!/bin/python

import json
import base64
import sys
import time
from kafka import KafkaProducer

import order_ops

extern_id = 0

user_id = 0
amount = 100

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
order_ops.place_market_order(producer, extern_id, False, user_id, amount)