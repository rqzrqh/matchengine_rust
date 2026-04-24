#!/bin/python

import json
import requests
from kafka import KafkaProducer

market = 'eth_btc'
topic = 'offer.{}'.format(market)

def get_input_sequence_id(domain='127.0.0.1', port='8080', market_name=None):
	if market_name is None:
		market_name = market
	payload = {'method': 'market.status', 'params': {'market': market_name}}
	url = 'http://{}:{}'.format(domain, port)
	r = requests.post(
		url,
		headers={'Content-Type': 'application/json'},
		data=json.dumps(payload),
		timeout=10,
	)
	r.raise_for_status()
	text = r.text.strip()
	if text == 'market not equal':
		raise ValueError(text)
	body = json.loads(text)
	if not isinstance(body, dict):
		raise ValueError('unexpected response: {}'.format(text))
	if 'error' in body:
		raise ValueError(body['error'])
	if 'input_sequence_id' not in body:
		raise ValueError('input_sequence_id missing: {}'.format(body))
	return int(body['input_sequence_id'])

def place_limit_order(producer, extern_id, isbuy, user_id, price, amount, input_sequence_id):

	side = 1

	if isbuy == True:
		side = 2
	else:
		side = 1

	data = dict()
	data['method'] = 'order.put_limit'
	data['id'] = extern_id
	data['input_sequence_id'] = input_sequence_id
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

def place_market_order(producer, extern_id, isbuy, user_id, amount, input_sequence_id):

	side = 1

	if isbuy == True:
		side = 2
	else:
		side = 1

	data = dict()
	data['method'] = 'order.put_market'
	data['id'] = extern_id
	data['input_sequence_id'] = input_sequence_id
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

def cancel_order(producer, extern_id, user_id, order_id, input_sequence_id):

	data = dict()
	data['method'] = 'order.cancel'
	data['id'] = extern_id
	data['input_sequence_id'] = input_sequence_id
	data['params'] = dict()
	data['params']['user_id'] = user_id
	data['params']['order_id'] = order_id

	json_str = json.dumps(data)
	print('', json_str)
	msg = bytes(json_str, encoding='utf8')
	producer.send(topic, msg)
	producer.flush()
