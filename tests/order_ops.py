#!/bin/python

import json
import base64
import sys
import time
from kafka import KafkaProducer

market = 'eth_btc'
topic = 'offer.{}'.format(market)

def place_limit_order(producer, extern_id, isbuy, user_id, price, amount):

	side = 1

	if isbuy == True:
		side = 2
	else:
		side = 1

	data = dict()
	data['method'] = 'order.put_limit'
	data['id'] = extern_id
	data['params'] = dict()
	data['params']['user_id'] = user_id
	data['params']['side'] = side
	data['params']['amount'] = str(amount)
	data['params']['price'] = str(price)
	data['params']['taker_fee_rate'] = '0.1'
	data['params']['maker_fee_rate'] = '0.1'

	json_str = json.dumps(data)
	print('', json_str)
	msg = bytes(json_str, encoding='utf8')
	producer.send(topic, msg)
	producer.flush()

def place_market_order(producer, extern_id, isbuy, user_id, amount):

	side = 1

	if isbuy == True:
		side = 2
	else:
		side = 1

	data = dict()
	data['method'] = 'order.put_market'
	data['id'] = extern_id
	data['params'] = dict()
	data['params']['user_id'] = user_id
	data['params']['side'] = side
	data['params']['amount'] = str(amount)
	data['params']['taker_fee_rate'] = '0.1'

	json_str = json.dumps(data)
	print('', json_str)
	msg = bytes(json_str, encoding='utf8')
	producer.send(topic, msg)
	producer.flush()

def cancel_order(producer, extern_id, user_id, order_id):

	data = dict()
	data['method'] = 'order.cancel'
	data['id'] = extern_id
	data['params'] = dict()
	data['params']['user_id'] = user_id
	data['params']['order_id'] = order_id

	json_str = json.dumps(data)
	print('', json_str)
	msg = bytes(json_str, encoding='utf8')
	producer.send(topic, msg)
	producer.flush()