#!/bin/python

import requests
import json
import base64
import sys
import time
from kafka import KafkaProducer
import order_ops

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
order_ops.place_limit_order(producer, 1, True, 1, 100, 10)
order_ops.place_limit_order(producer, 2, False, 2, 1000, 1)
order_ops.place_market_order(producer, 3, True, 3, 1000)
order_ops.place_market_order(producer, 4, False, 4, 2000)
order_ops.cancel_order(producer, 5, 20, 100)