#!/usr/bin/env python
from processor import DashboardProcessor
from config import AGGREGATED_CARDS_BY_PURCHASES_SHOPS, AGGREGATED_RE_PURCHASES_SHOPS, \
	AGGREGATED_RE_PURCHASES_TOTALS_SHOPS, AGGREGATED_CHEQUES_PER_PERIOD
from datetime import datetime, timedelta
import time


class CustomersByPurchases(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(CustomersByPurchases, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'customers_by_purchases'
		self.target_collection = AGGREGATED_CARDS_BY_PURCHASES_SHOPS
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}}
			},
			{
				"$group": {
					"_id": {"shop_id": "$shop_id", "card_number": '$card_number'},
					"cheques": {"$sum": 1},
				}
			},
			{
				"$group": {
					"_id": {
						'qty': {"$cond": [{"$gte": ['$cheques', 3]}, 3, '$cheques']},
						'shop_id': '$_id.shop_id'
					},
					"count": {"$sum": 1},
				}
			},
			{
				"$group": {
					"_id": {
						'shop_id': '$_id.shop_id'
					},
					"counts": {
						"$addToSet": {"qty": "$_id.qty", "cards": "$count"}
					}
				}
			}
		]

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			cursor = self.db[collection_name].find()
			for data in cursor:
				key = str(data['_id']['shop_id'])
				try:
					_ = result[key]
				except KeyError:
					result[key] = {
						'_id': key,
						'shop_id': data['_id']['shop_id'],
						'qty_0': 0, 'qty_1': 0, 'qty_2': 0, 'qty_3': 0,
					}
				finally:
					for q in data['counts']:
						result[key]['qty_' + str(q['qty'])] += q['cards']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		if data:
			self.db[self.target_collection].insert(data)


class RePurchasesMonth(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonth, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'repurchases_month'
		self.target_collection = AGGREGATED_RE_PURCHASES_SHOPS
		month_start = datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts = int(time.mktime(month_start.timetuple()))
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}, "date": {"$gte": month_start_ts}}
			},
			{
				"$group": {
					"_id": {"shop_id": '$shop_id', 'card_number': '$card_number'},
					"diff": {"$first": {"$subtract": ['$sum', '$return_sum']}},
					"date": {"$min": "$date"}
				}
			},
			{
				"$group": {
					"_id": '$_id.shop_id',
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
						'shop_id': data['_id'],
						'not_firsts': 0
					}
				finally:
					result[key]['not_firsts'] += data['result']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		if data:
			self.db[self.target_collection].insert(data)


class RePurchasesMonthTotals(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonthTotals, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'repurchases_month_totals'
		self.target_collection = AGGREGATED_RE_PURCHASES_TOTALS_SHOPS
		month_start = datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts = int(time.mktime(month_start.timetuple()))
		self.pipeline = [
			{
				"$match": {"date": {"$gte": month_start_ts}}
			},
			{
				"$group": {
					"_id": '$shop_id',
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
						'shop_id': data['_id'],
						'has_card': 0,
						'no_card': 0,
					}
				finally:
					result[key]['has_card'] += data['has_card']
					result[key]['no_card'] += data['no_card']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		if data:
			self.db[self.target_collection].insert(data)


class ChequesPerPeriodProcessor(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(ChequesPerPeriodProcessor, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'cheques_per_period'
		self.target_collection = AGGREGATED_CHEQUES_PER_PERIOD
		today = datetime.today().replace(hour=0, minute=0, second=0)
		yesterday = today - timedelta(days=1)
		yesterday_ts = int(time.mktime(yesterday.timetuple()))
		week_start = today - timedelta(days=today.weekday())
		week_start_ts = int(time.mktime(week_start.timetuple()))
		month_start = datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts = int(time.mktime(month_start.timetuple()))
		self.pipeline = [
			{
				"$match": {"date": {"$gte": month_start_ts}}
			},
			{
				"$group": {
					'_id': '$shop_id',
					"brand_id": {"$first": "$brand_id"},
					"month": {"$sum": 1},
					"week": {"$sum": {"$cond": [{'$gte': ['$date', week_start_ts]}, 1, 0]}},
					"day": {"$sum": {"$cond": [{'$gte': ['$date', yesterday_ts]}, 1, 0]}}
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
						'shop_id': data['_id'],
						'brand_id': data['brand_id'],
						'month': 0,
						'week': 0,
						'day': 0
					}
				finally:
					result[key]['month'] += data['month']
					result[key]['week'] += data['week']
					result[key]['day'] += data['day']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		if data:
			self.db[self.target_collection].insert(data)
