#!/bin/python

import requests
import json
import base64
import sys
import time

market_name = "eth_btc"
domain="127.0.0.1"
port="8080"

data1 = dict()
data1["method"] = "market.user_order_pending"
data1["params"] = dict()
data1["params"]["market"] = market_name
data1["params"]["user_id"] = 1
data1["params"]["offset"] = 0
data1["params"]["limit"] = 100

url_1 = "http://" + domain + ":" + str(port)
header = {'Content-Type': 'application/json'}
response = requests.post(url=url_1, headers=header, data=json.dumps(data1))
print(response.text)
