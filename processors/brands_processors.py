#!/usr/bin/env python
from processor import DashboardProcessor
from datetime import datetime, timedelta
from config import AGGREGATED_CARDS_BY_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_TOTALS_BRANDS
import time

class BrandsCustomersByPurchases(DashboardProcessor):
	""" """
	def __init__(self, map_key_name, brand_id, start_ts=None, end_ts=None):
		super(BrandsCustomersByPurchases,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'brands_customers_by_purchases'
		self.pipeline = [
	    {"$match": {"card_number": {"$ne": ''}}},
	    {"$match": {"brand_id": {"$eq": brand_id}}},
	    {"$group": {
	        "_id": { "card_number": '$card_number' },
	        "card_number": { "$first": '$card_number' },
	        "cheques": { "$sum": 1 }
	    }},
	    {"$group": {
	        "_id": { "$cond": [{"$gte": ['$cheques', 3]}, 3, '$cheques']},
	        "count": { "$sum": 1 }
	    }},
		]	

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			q = self.db[collection_name].aggregate({
				"$group": {
					"_id": None,
					"purchases": {
						"$push": {"qty": "$_id", "cards": "$count"}
					}
				}
			})
			if not q['result']:
				continue
			purchases = q['result'][0]['purchases']
			for v in purchases:
				try:
					_ = result[str(v['qty'])]
				except KeyError:
					result[str(v['qty'])] = 0
				finally:
					result[str(v['qty'])] += v['cards']
		return result
		

class BrandsRePurchases(DashboardProcessor):	
	""" """

	def __init__(self, map_key_name, brand_id, start_ts=None, end_ts=None):
		super(BrandsRePurchases,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'brand_repurchases'
		self.pipeline = [
				{"$match": {"card_number": {"$ne": ''}, "date": {"$gte": start_ts}, "brand_id": brand_id}},
				{"$group": {
						"_id": '$card_number',
						"diff": {"$first": {"$subtract": ['$sum', '$return_sum']}},
						"date": {"$min": "$date"}
				}},
				{"$group": {
						"_id": None,
						"result": { "$sum": "$diff" }
				}},
		]

	def process_results(self):
		result = 0
		for collection_name in self.collections:
			q = self.db[collection_name].aggregate({
				"$project": {
					"sum": "$result"
				}
			})
			purchases = 0 if not q['result'] else q['result'][0]['sum']
			result += purchases
		return {"not_firsts": result}


class BrandsRePurchasesTotals(DashboardProcessor):	
	""" """

	def __init__(self, map_key_name, brand_id, start_ts=None, end_ts=None):
		super(BrandsRePurchasesTotals,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'brand_repurchases_totals'
		self.pipeline = [
				{"$match": {"card_number": {"$ne": ''}, "date": {"$gte": start_ts}, "brand_id": brand_id}},
				{"$group": {
						"_id": None,
						"has_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, 0, {'$subtract': ['$sum', '$return_sum']}]}},
						"no_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, {'$subtract': ['$sum', '$return_sum']}, 0]}},
				}}
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


class CustomersByPurchases(DashboardProcessor):
	""" """
	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(CustomersByPurchases,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'customers_by_purchases'
		self.target_collection = AGGREGATED_CARDS_BY_PURCHASES_BRANDS
		self.pipeline = [
	    {"$match": {"card_number": {"$ne": ''}}},
	    {"$group": {
	        "_id": { "brand_id": "$brand_id", "card_number": '$card_number' },
	        "cheques": { "$sum": 1 },
	    }},
	    {"$group": {
	        "_id": { 
	        	'qty': {"$cond": [{"$gte": ['$cheques', 3]}, 3, '$cheques']}, 
	        	'brand_id': '$_id.brand_id'
        	},
	        "count": { "$sum": 1 },
	    }},
	    {"$group": {
	    		"_id": {
	        	'brand_id': '$_id.brand_id'	    			
	    		},
	    		"counts": {
	    			"$addToSet": {"qty": "$_id.qty", "cards": "$count"}
	    		}
	    }}
		]	

	def process_results(self):
		result = {}
		for collection_name in self.collections:
			cursor = self.db[collection_name].find()
			for data in cursor:
				key = str(data['_id']['brand_id'])
				try:
					_ = result[key]
				except KeyError:
					result[key] = {
						'_id': key,
						'brand_id': data['_id']['brand_id'],
						'qty_0': 0, 'qty_1': 0, 'qty_2': 0, 'qty_3': 0,
					}	
				finally:
					for q in data['counts']:
						result[key]['qty_' + str(q['qty'])] += q['cards']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		self.db[self.target_collection].insert(data)


class RePurchasesMonth(DashboardProcessor):
	""" """
	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonth,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'repurchases_month'
		self.target_collection = AGGREGATED_RE_PURCHASES_BRANDS
		month_start 		= datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts 	= int(time.mktime(month_start.timetuple()))
		self.pipeline = [
				{"$match": {"card_number": {"$ne": ''}, "date": {"$gte": month_start_ts}}},
				{"$group": {
						"_id": 	{"brand_id": '$brand_id', 'card_number': '$card_number'},
						"diff": {"$first": {"$subtract": ['$sum', '$return_sum']}},
						"date": {"$min": "$date"}
				}},
				{"$group": {
						"_id": '$_id.brand_id',
						"result": { "$sum": "$diff" }
				}},
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
						'not_firsts': 0
					}	
				finally:
					result[key]['not_firsts'] += data['result']
		return result.values()

	def save_data(self, data):
		self.db[self.target_collection].drop()
		self.db[self.target_collection].insert(data)


class RePurchasesMonthTotals(DashboardProcessor):
	""" """
	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		super(RePurchasesMonthTotals,self).__init__(map_key_name, start_ts, end_ts)
		self.name 		= 'repurchases_month_totals'
		self.target_collection = AGGREGATED_RE_PURCHASES_TOTALS_BRANDS
		month_start 		= datetime(datetime.today().year, datetime.today().month, 1)
		month_start_ts 	= int(time.mktime(month_start.timetuple()))
		self.pipeline = [
				{"$match": {"date": {"$gte": month_start_ts}}},
				{"$group": {
						"_id": '$brand_id',
						"has_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, 0, {'$subtract': ['$sum', '$return_sum']}]}},
						"no_card": {"$sum": {"$cond": [{'$eq': ['$card_number', '']}, {'$subtract': ['$sum', '$return_sum']}, 0]}},
				}}
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
