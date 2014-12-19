#!/usr/bin/env python
# -*- coding: utf-8 -*-
from processor import DashboardProcessor
from datetime import datetime, timedelta
from config import AGGREGATED_RE_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_TOTALS_BRANDS
import time


class RePurchasesMonth(DashboardProcessor):
	""" Сумма всех первых продаж """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonth, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'repurchases_month'
		self.target_collection = AGGREGATED_RE_PURCHASES_BRANDS
		month_start = datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts = int(time.mktime(month_start.timetuple()))
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}, "date": {"$gte": month_start_ts}}
			},
			{
				"$group": {
					"_id": {"brand_id": '$brand_id', 'card_number': '$card_number'},
					"diff": {"$first": {"$subtract": ['$sum', '$return_sum']}},
					"date": {"$min": "$date"}
				}
			},
			{
				"$group": {
					"_id": '$_id.brand_id',
					"result": {"$sum": "$diff"}
				}
			},
		]

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			cursor = self.db[collection_name].find()
			for data in cursor:
				key = str(data['_id'])
				try:
					_ = result[key]
				except KeyError:
					result[key] = {
						'_id': key,
						'brand_id': data['_id'],
						'firsts': 0
					}
				finally:
					result[key]['firsts'] += data['result']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		self.db[self.target_collection].insert(data)


class RePurchasesMonthTotals(DashboardProcessor):
	""" Сумма всех продаж по картам и без них """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonthTotals, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'repurchases_month_totals'
		self.target_collection = AGGREGATED_RE_PURCHASES_TOTALS_BRANDS
		month_start = datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts = int(time.mktime(month_start.timetuple()))
		self.pipeline = [
			{
				"$match": {"date": {"$gte": month_start_ts}}
			},
			{
				"$group": {
					"_id": '$brand_id',
					"has_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, 0, {'$subtract': ['$sum', '$return_sum']}]}},
					"no_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, {'$subtract': ['$sum', '$return_sum']}, 0]}},
				}
			}
		]

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			cursor = self.db[collection_name].find()
			for data in cursor:
				key = str(data['_id'])
				try:
					_ = result[key]
				except KeyError:
					result[key] = {
						'_id': key,
						'brand_id': data['_id'],
						'has_card': 0,
						'no_card': 0,
					}
				finally:
					result[key]['has_card'] += data['has_card']
					result[key]['no_card'] += data['no_card']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		self.db[self.target_collection].insert(data)
