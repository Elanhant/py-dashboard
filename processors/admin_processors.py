#!/usr/bin/env python
from processor import DashboardProcessor
from multiprocessing import Process
from datetime import datetime, timedelta
from config import WEEKS_SALES_COLLECTION_PREFIX
import time, copy

current_milli_time = lambda: int(round(time.time() * 1000))


class AdminRePurchases(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(AdminRePurchases, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'admin_repurchases'
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}, "date": {"$gte": start_ts}}
			},
			{
				"$group": {
					"_id": '$card_number',
					"diff": {"$first": {"$subtract": ['$sum', '$return_sum']}},
					"date": {"$min": "$date"}
				}
			},
			{
				"$group": {
					"_id": None,
					"result": {"$sum": "$diff"}
				}
			},
		]

	def process_results(self):
		result = 0
		for collection_name in self.collections:
			q = self.db[collection_name].aggregate({
				"$project": {"sum": "$result"}
			})
			purchases = 0 if not q['result'] else q['result'][0]['sum']
			result += purchases
		return {"not_firsts": result}


class AdminRePurchasesTotals(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(AdminRePurchasesTotals, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'admin_repurchases_totals'
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}, "date": {"$gte": start_ts}}
			},
			{
				"$group": {
					"_id": None,
					"has_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, 0, {'$subtract': ['$sum', '$return_sum']}]}},
					"no_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, {'$subtract': ['$sum', '$return_sum']}, 0]}},
				}
			}
		]

	def process_results(self):
		result = {'has_card': 0, 'no_card': 0}
		for collection_name in self.collections:
			q = self.db[collection_name].aggregate({
				"$project": {
					"has_card": "$has_card",
					"no_card": "$no_card",
				}
			})
			purchases = {'has_card': 0, 'no_card': 0} if not q['result'] else q['result'][0]
			result['has_card'] += purchases['has_card']
			result['no_card'] += purchases['no_card']
		return result


class AdminCustomersByPurchases(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(AdminCustomersByPurchases, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'admin_customers_by_purchases'
		self.pipeline = [
			{
				"$match": {"card_number": {"$ne": ''}}
			},
			{
				"$group": {
					"_id": {"brand_id": '$brand_id', "card_number": '$card_number'},
					"card_number": {"$first": '$card_number'},
					"cheques": {"$sum": 1},
					"last_ts": {"$max": "$date"}
				}
			},
			{
				"$group": {
					"_id": {"$cond": [{"$gte": ['$cheques', 3]}, 3, '$cheques']},
					"count": {"$sum": 1},
					"last_ts": {"$max": "$last_ts"}
				}
			},
		]

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			q = self.db[collection_name].aggregate({
				"$project": {
					"cards": "$count"
				}
			})
			purchases = q['result']
			for data in purchases:
				key = str(data['_id'])
				try:
					_ = result[key]
				except KeyError:
					result[key] = 0
				finally:
					result[key] += data['cards']
		return result


class AdminSalesEightWeeks(DashboardProcessor):
	""" """

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(AdminSalesEightWeeks, self).__init__(map_key_name, start_ts, end_ts)
		self.name = 'admin_sales_eight_weeks'
		self.target_collection = 'dashboard_sales'
		self.subset_collection = WEEKS_SALES_COLLECTION_PREFIX
		self.pipeline = [
			{
				'$group': {
					'_id': {'brand_id': '$brand_id', 'shop_id': '$shop_id'},
					'has_card': {
						'$sum': {
							'$cond': [{'$eq': ['$card_number', '']}, 0, {'$subtract': ['$sum', '$return_sum']}]
						}
					},
					'no_card': {
						'$sum': {
							'$cond': [{'$eq': ['$card_number', '']}, {'$subtract': ['$sum', '$return_sum']}, 0]
						}
					},
					'overall': {
						'$sum': {
							'$subtract': ['$sum', '$return_sum']
						}
					},
				}
			}
		]

	def run(self, split_keys=None):
		if self.DEBUG:
			print "===== Dashboard processor started at %s" % time.ctime()
		t_start = current_milli_time()
		start = t_start
		self.run_threads()
		if self.DEBUG:
			print "===== Running threads has taken %s ms to complete" % (current_milli_time() - t_start)
		t_start = current_milli_time()
		if self.DEBUG:
			print "===== Processing results has taken %s ms to complete" % (current_milli_time() - t_start)
		t_start = current_milli_time()
		# self.drop_subset_collections()
		if self.DEBUG:
			print "===== Dropping subset collections has taken %s ms to complete" % (current_milli_time() - t_start)
			print "===== Processing was completed in %s ms" % (current_milli_time() - start)

	def run_threads(self):
		t_start = current_milli_time()
		today = datetime.today().replace(hour=0, minute=0, second=0)
		week_start = today - timedelta(days=today.weekday(), weeks=7)
		week_start_ts = int(time.mktime(week_start.timetuple()))

		for i in range(2):
			for w in range(self.NUM_THREADS):
				week_n = w + i * self.NUM_THREADS
				start = today - timedelta(days=today.weekday(), weeks=week_n)
				start_ts = int(time.mktime(start.timetuple()))
				end = today - timedelta(days=today.weekday(), weeks=week_n - 1)
				end_ts = int(time.mktime(end.timetuple()))

				self.collections.append(self.subset_collection + str(week_n))
				t = Process(target=self.aggregate_subset, args=(start_ts, end_ts, week_n))
				self.threads.append(t)
				t.start()

		for t in self.threads:
			t.join()