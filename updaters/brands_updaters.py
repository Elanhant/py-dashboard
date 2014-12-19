#!/usr/bin/env python
# -*- coding: utf-8 -*-
from updater import Updater
from processors.brands_processors import RePurchasesMonth, RePurchasesMonthTotals
from datetime import datetime, timedelta
from config import DASHBOARD_CHEQUES_COLLECTION
import time


class PurchasesWithCardsUpdater(Updater):
	""" Доля продаж по картам """

	def __init__(self, brand_id, start_ts, end_ts):
		super(PurchasesWithCardsUpdater, self).__init__()
		self.name = 'brands_purchases_with_cards_' + str(brand_id)
		self.brand_id = brand_id
		self.start_ts = start_ts
		self.end_ts = end_ts

	def calculate_total_data(self):
		denominator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'brand_id': self.brand_id,
			'date': {'$gte': self.start_ts, '$lt': self.end_ts}
		}).count()
		numerator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'brand_id': self.brand_id,
			'date': {'$gte': self.start_ts, '$lt': self.end_ts},
			'card_number': {'$ne': ''}
		}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		denominator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'brand_id': self.brand_id,
			'date': {'$gte': yesterday_ts, '$lt': today_ts}
		}).count()
		numerator = self.db[DASHBOARD_CHEQUES_COLLECTION].find({
			'brand_id': self.brand_id,
			'date': {'$gte': yesterday_ts, '$lt': today_ts},
			'card_number': {'$ne': ''}
		}).count()
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'],
		                            denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class BonusesUpdater(Updater):
	""" Продажи за последние 8 недель """

	def __init__(self, brand_id, start_ts):
		super(BonusesUpdater, self).__init__()
		self.name = 'brands_bonuses_' + str(brand_id)
		self.brand_id = brand_id
		self.start_ts = start_ts

	def calculate_total_data(self):
		result = self.db[DASHBOARD_CHEQUES_COLLECTION].aggregate([
			{
				'$match': {
					'date': {'$gte': self.start_ts},
					'brand_id': {'$eq': self.brand_id}
				}
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
		result = self.db[DASHBOARD_CHEQUES_COLLECTION].aggregate([
			{
				'$match': {
					'date': {'$gte': self.start_ts},
					'brand_id': {'$eq': self.brand_id}
				}
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
		result = self.db[DASHBOARD_CHEQUES_COLLECTION].aggregate([
			{
				'$match': {
					'date': {'$gte': yesterday_ts},
					'brand_id': {'$eq': self.brand_id}
				}
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
		result = self.db[DASHBOARD_CHEQUES_COLLECTION].aggregate([
			{
				'$match': {
					'date': {'$gte': yesterday_ts},
					'brand_id': {'$eq': self.brand_id}
				}
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


class RePurchasesMonthUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """

	def __init__(self, split_keys=None):
		super(RePurchasesMonthUpdater, self).__init__()
		self.name = 'repurchases_month'
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
		self.name = 'repurchases_month_totals'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = RePurchasesMonthTotals('card_number')
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()