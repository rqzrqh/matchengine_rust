#!/bin/python

import requests
import json
import base64
import sys
import time
from kafka import KafkaProducer

market = 'eth_btc'
topic = 'offer.{}'.format(market)

extern_oper_id = 0

user_id = 0
order_id = 100

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
data = dict()
data['method'] = 'order.cancel'
data['id'] = extern_oper_id
data['params'] = dict()
data['params']['user_id'] = user_id
data['params']['order_id'] = order_id

json_str = json.dumps(data)
print('', json_str)
msg = bytes(json_str, encoding='utf8')
producer.send(topic, msg)
producer.flush()