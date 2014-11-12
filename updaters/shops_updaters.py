#!/usr/bin/env python
# -*- coding: utf-8 -*-
from updater import Updater
from processors.shops_processors import CustomersByPurchases, RePurchasesMonth, RePurchasesMonthTotals, ChequesPerPeriodProcessor
from datetime import datetime, timedelta
from config import DASHBOARD_CHEQUES_COLLECTION
import time


class PurchasesWithCardsUpdater(Updater):
	""" Доля продаж по картам """
	def __init__(self, shop_id, start_ts, end_ts):
		super(PurchasesWithCardsUpdater, self).__init__()
		self.name 		= 'shops_purchases_with_cards_' + str(shop_id)
		self.shop_id 	= shop_id
		self.start_ts = start_ts
		self.end_ts 	= end_ts
		
	def calculate_total_data(self):
		denominator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'shop_id': 	self.shop_id,
			'date': 		{'$gte': self.start_ts, '$lt': self.end_ts}
			}).count()
		numerator 	= self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'shop_id': 			self.shop_id,
			'date': 				{'$gte': self.start_ts, '$lt': self.end_ts},
			'card_number': 	{'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		denominator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'shop_id': 	self.shop_id,
			'date': 		{'$gte': yesterday_ts, '$lt': today_ts}
			}).count()
		numerator 	= self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'shop_id': 			self.shop_id,
			'date': 				{'$gte': yesterday_ts, '$lt': today_ts},
			'card_number': 	{'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class CustomersByPurchasesUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None):
		super(CustomersByPurchasesUpdater, self).__init__()
		self.name 			= 'customers_by_purchases'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = CustomersByPurchases('card_number')	
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()


class RePurchasesMonthUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None):
		super(RePurchasesMonthUpdater, self).__init__()
		self.name 			= 'repurchases_month'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = RePurchasesMonth('card_number')	
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()


class RePurchasesMonthTotalsUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None):
		super(RePurchasesMonthTotalsUpdater, self).__init__()
		self.name 			= 'repurchases_month_totals'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = RePurchasesMonthTotals('card_number')	
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()


class ChequesPerPeriod(Updater):
	""" Число чеков за месяц, неделю, прошедший день """
	def __init__(self, split_keys=None):
		super(ChequesPerPeriod, self).__init__()
		self.name 			= 'cheques_per_period'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = ChequesPerPeriodProcessor('card_number')	
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()