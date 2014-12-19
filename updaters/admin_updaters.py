#!/usr/bin/env python
# -*- coding: utf-8 -*-
from updater import Updater
from processors.admin_processors import AdminRePurchases, AdminRePurchasesTotals, AdminSalesEightWeeks
from datetime import datetime, timedelta
import time

current_milli_time = lambda: int(round(time.time() * 1000))


class RePurchasesUpdater(Updater):
	""" Доля повторных покупок """

	def __init__(self, start_ts=0, name='admin_repurchases', split_keys=None):
		super(RePurchasesUpdater, self).__init__()
		self.name = name
		self.start_ts = start_ts
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = AdminRePurchases('card_number', self.start_ts)
		processor.run(self.split_keys)
		numerator = processor.result
		processor = AdminRePurchasesTotals('card_number', self.start_ts)
		processor.run(self.split_keys)
		denominator = processor.result
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		processor = AdminRePurchases('card_number', today_ts, yesterday_ts)
		processor.run(self.split_keys)
		numerator = processor.result
		processor = AdminRePurchasesTotals('card_number', today_ts, yesterday_ts)
		processor.run(self.split_keys)
		denominator = processor.result
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += numerator[k]
			except KeyError:
				pass
		for k in self.aggregated_data['denominator']:
			try:
				self.aggregated_data['denominator'][k] += denominator[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class PurchasesWithCardsUpdater(Updater):
	""" Доля продаж по картам """

	def __init__(self, start_ts, end_ts):
		super(PurchasesWithCardsUpdater, self).__init__()
		self.name = 'admin_purchases_with_cards'
		self.start_ts = start_ts
		self.end_ts = end_ts

	def calculate_total_data(self):
		denominator = self.db['dashboard_cheques'].find({
		'date': {'$gte': self.start_ts, '$lt': self.end_ts}
		}).count()
		numerator = self.db['dashboard_cheques'].find({
		'date': {'$gte': self.start_ts, '$lt': self.end_ts},
		'card_number': {'$ne': ''}
		}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		denominator = self.db['dashboard_cheques'].find({
		'date': {'$gte': yesterday_ts, '$lt': today_ts}
		}).count()
		numerator = self.db['dashboard_cheques'].find({
		'date': {'$gte': yesterday_ts, '$lt': today_ts},
		'card_number': {'$ne': ''}
		}).count()
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'],
		                            denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class BonusesUpdater(Updater):
	""" Доли начислений и списаний """

	def __init__(self, start_ts):
		super(BonusesUpdater, self).__init__()
		self.name = 'admin_bonuses'
		self.start_ts = start_ts

	def calculate_total_data(self):
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {'date': {'$gte': self.start_ts}}
			},
			{
				'$group': {
					'_id': None,
					'accrual': {'$sum': '$accrual'},
					'used': {'$sum': '$used'},
				}
			}
		])
		numerator = {
		'accrual': 0 if not result['result'] else result['result'][0]['accrual'],
		'used': 0 if not result['result'] else result['result'][0]['used'],
		}
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {'date': {'$gte': self.start_ts}}
			},
			{
				'$group': {
					'_id': None,
					'sum': {'$sum': '$sum'}
				}
			}
		])
		denominator = 0 if not result['result'] else result['result'][0]['sum']
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {'date': {'$gte': yesterday_ts}}
			},
			{
				'$group': {
					'_id': None,
					'accrual': {'$sum': '$accrual'},
					'used': {'$sum': '$used'},
				}
			}
		])
		numerator = {
		'accrual': self.aggregated_data['numerator']['accrual'] if not result['result'] else result['result'][0][
			                                                                                     'accrual'] +
		                                                                                     self.aggregated_data[
			                                                                                     'numerator'][
			                                                                                     'accrual'],
		'used': self.aggregated_data['numerator']['used'] if not result['result'] else result['result'][0]['used'] +
		                                                                               self.aggregated_data[
			                                                                               'numerator']['used'],
		}
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {'date': {'$gte': yesterday_ts}}
			},
			{
				'$group': {
					'_id': None,
					'sum': {'$sum': '$sum'}
				}
			}
		])
		denominator = 0 if not result['result'] else result['result'][0]['sum']

		self.update_aggregated_data(numerator, denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class SalesUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """

	def __init__(self, split_keys=None, last_ts=0):
		super(SalesUpdater, self).__init__()
		self.name = 'admin_sales'
		self.split_keys = split_keys
		self.last_ts = last_ts

	def calculate_total_data(self):
		processor = AdminSalesEightWeeks('date')
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()