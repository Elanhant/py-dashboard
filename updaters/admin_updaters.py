#!/usr/bin/env python
# -*- coding: utf-8 -*-
from updater import Updater
from processors.admin_processors import AdminCustomersByPurchases, AdminRePurchases, AdminRePurchasesTotals, AdminSalesEightWeeks
from datetime import datetime, timedelta
import time

current_milli_time = lambda: int(round(time.time() * 1000))

class CustomersByPurchasesUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None):
		super(CustomersByPurchasesUpdater, self).__init__()
		self.name 			= 'admin_customers_by_purchases'
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = AdminCustomersByPurchases('card_number')	
		processor.run(self.split_keys)
		result = processor.result
		self.cur.execute(""" SELECT (SELECT COUNT('id') FROM card) + (SELECT COUNT('id') FROM online_data) """)
		customers_count = self.cur.fetchone()[0]
		self.update_aggregated_data(result, customers_count)
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		processor = AdminCustomersByPurchases('card_number', start_ts=today_ts, end_ts=yesterday_ts)
		processor.run(self.split_keys)
		result = processor.result	
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE date_create >= %s """, (yesterday_ts,))
		customers_count = self.cur.fetchone()[0]
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += result[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], customers_count + self.aggregated_data['denominator'])
		return self.aggregated_data['numerator']


class RePurchasesUpdater(Updater):
	""" Доля повторных покупок """
	def __init__(self, start_ts=0, name='admin_repurchases', split_keys=None):
		super(RePurchasesUpdater, self).__init__()
		self.name 			= name
		self.start_ts 	= start_ts
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


class CustomersDataUpdater(Updater):
	""" Соотношение каналов коммуникации и доля согласных на рассылку """
	def __init__(self):
		super(CustomersDataUpdater, self).__init__()
		self.name = 'admin_customers_data'

	def calculate_total_data(self):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN subscription THEN 1 ELSE 0 END)::FLOAT as subscribed
			FROM card """)
		result = self.cur.fetchone()
		numerator = {
			'all': 				0 if result[0] is None else result[0],
			'email': 			0 if result[1] is None else result[1],
			'sms': 				0 if result[2] is None else result[2],	
			'unknown': 		0 if result[3] is None else result[3],	
			'subscribed': 0 if result[4] is None else result[4]
		}
		self.cur.execute(""" SELECT (SELECT COUNT('id') FROM card) + (SELECT COUNT('id') FROM online_data) """)
		denominator = self.cur.fetchone()[0]		
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN subscription THEN 1 ELSE 0 END)::FLOAT as subscribed
			FROM card WHERE date_create >= %s AND date_create < %s""", (yesterday_ts, today_ts))
		result = self.cur.fetchone()
		numerator = {
			'all': 				0 if result[0] is None else result[0],
			'email': 			0 if result[1] is None else result[1],
			'sms': 				0 if result[2] is None else result[2],	
			'unknown': 		0 if result[3] is None else result[3],
			'subscribed': 0 if result[4] is None else result[4]
		}
		self.cur.execute(""" SELECT (SELECT COUNT('id') FROM card WHERE date_create >= %s) + (SELECT COUNT('id') FROM online_data) """, (yesterday_ts, ))
		denominator = self.cur.fetchone()[0]		
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += numerator[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class GendersDataUpdater(Updater):
	""" Соотношение полов """
	def __init__(self):
		super(GendersDataUpdater, self).__init__()
		self.name = 'admin_genders_data'

	def calculate_total_data(self):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(digital_id) as total FROM personal_data """)
		result = self.cur.fetchone()
		numerator = {
			'male': 			0 if result[0] is None else result[0],
			'female': 		0 if result[1] is None else result[1],
			'undefined': 	0 if result[2] is None else result[2],	
		}
		denominator = 	0 if result[3] is None else result[3]
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total 
			FROM customer t JOIN personal_data p
			ON p.digital_id = t.digital_id 
			WHERE t.date >= %s AND t.date < %s """, (yesterday_ts, today_ts))
		result = self.cur.fetchone()
		numerator = {
			'male': 			0 if result[0] is None else result[0],
			'female': 		0 if result[1] is None else result[1],
			'undefined': 	0 if result[2] is None else result[2],	
		}
		denominator = 	0 if result[3] is None else result[3]
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += numerator[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		
class ParticipantsCountUpdater(Updater):
	""" Число участников """
	def __init__(self,):
		super(ParticipantsCountUpdater, self).__init__()
		self.name = 'admin_participants_count'
		
	def calculate_total_data(self):
		numerator = 0
		self.cur.execute(""" SELECT COUNT(id) FROM card """)
		result = self.cur.fetchone()
		result = 0 if result[0] is None else result[0]
		numerator += result
		self.cur.execute(""" SELECT COUNT(id) FROM online_data """)
		result = self.cur.fetchone()
		result = 0 if result[0] is None else result[0]
		numerator += result
		self.cur.execute(""" SELECT COUNT(id) FROM subscriber """)
		result = self.cur.fetchone()
		result = 0 if result[0] is None else result[0]
		numerator += result

		denominator = 1
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT COUNT(id) FROM card WHERE date_create >= %s AND date_create < %s """, (yesterday_ts, today_ts))
		result = self.cur.fetchone()
		numerator = 0 if result[0] is None else result[0]
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class PurchasesWithCardsUpdater(Updater):
	""" Доля продаж по картам """
	def __init__(self, start_ts, end_ts):
		super(PurchasesWithCardsUpdater, self).__init__()
		self.name 		= 'admin_purchases_with_cards'
		self.start_ts = start_ts
		self.end_ts 	= end_ts
		
	def calculate_total_data(self):
		denominator = self.db['dashboard_cheques'].find({
			'date': {'$gte': self.start_ts, '$lt': self.end_ts}
			}).count()
		numerator 	= self.db['dashboard_cheques'].find({
			'date': {'$gte': self.start_ts, '$lt': self.end_ts},
			'card_number': {'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		denominator = self.db['dashboard_cheques'].find({
			'date': {'$gte': yesterday_ts, '$lt': today_ts}
			}).count()
		numerator 	= self.db['dashboard_cheques'].find({
			'date': {'$gte': yesterday_ts, '$lt': today_ts},
			'card_number': {'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		
class SalesEightWeeksUpdater(Updater):
	""" Продажи за последние 8 недель """
	def __init__(self):
		super(SalesEightWeeksUpdater, self).__init__()
		self.name = 'admin_eight_weeks_sales'
		
	def calculate_total_data(self):		
		t_start 			= current_milli_time()
		today 				= datetime.today().replace(hour=0, minute=0, second=0)
		week_start 		= today - timedelta(days=today.weekday(), weeks=7)
		week_start_ts = int(time.mktime(week_start.timetuple()))
		numerator 		= []
		for w in range(8):
			start 		= today - timedelta(days=today.weekday(), weeks=w)
			start_ts 	= int(time.mktime(start.timetuple()))
			end 			= today - timedelta(days=today.weekday(), weeks=w-1)
			end_ts 		= int(time.mktime(end.timetuple()))
			pipeline	= [
				{
					'$match': {
						'$and': [{'date': {'$gt': start_ts}}, {'date': {'$lte': end_ts}}]
					}
				},
				{
					'$group': {
						'_id': None,
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
						}
					}
				}
			]
			result = self.db['dashboard_cheques'].aggregate(pipeline)['result']
			numerator.append({
				'label': 		'Неделя ' + str(start.isocalendar()[1]),
				'has_card': 0 if not result else result[0]['has_card'],
				'no_card': 	0 if not result else result[0]['no_card']
				})

		result 			= self.db['dashboard_cheques'].aggregate(pipeline)['result']
		denominator = 1
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts=0, yesterday_ts=0):
		self.calculate_total_data()
		

class BonusesUpdater(Updater):
	""" Доли начислений и списаний """
	def __init__(self, start_ts):
		super(BonusesUpdater, self).__init__()
		self.name 		= 'admin_bonuses'
		self.start_ts = start_ts
		
	def calculate_total_data(self):
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': {'$gte': self.start_ts}
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
			'accrual': 	0 if not result['result'] else result['result'][0]['accrual'],
			'used': 		0 if not result['result'] else result['result'][0]['used'],
		}
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': {'$gte': self.start_ts}
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
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': {'$gte': yesterday_ts}
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
			'accrual': 	self.aggregated_data['numerator']['accrual'] if not result['result'] else result['result'][0]['accrual'] + self.aggregated_data['numerator']['accrual'],
			'used': 		self.aggregated_data['numerator']['used'] if not result['result'] else result['result'][0]['used'] + self.aggregated_data['numerator']['used'],
		}
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': {'$gte': yesterday_ts}
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
		

class ParticipantsByBrandsUpdater(Updater):
	""" Число участников в разрезе брендов """
	def __init__(self,):
		super(ParticipantsByBrandsUpdater, self).__init__()
		self.name = 'admin_participants_by_brands'
		
	def calculate_total_data(self):
		self.cur.execute(""" SELECT COUNT(id), company_id FROM card GROUP BY company_id """)
		cards = self.cur.fetchall() or [(0,0)]
		self.cur.execute(""" SELECT COUNT(id), company_id FROM online_data GROUP BY company_id """)
		onlines = self.cur.fetchall() or [(0,0)]
		self.cur.execute(""" SELECT COUNT(id), company_id FROM subscriber GROUP BY company_id """)
		subscribers = self.cur.fetchall() or [(0,0)]

		data = self.format_data(cards, onlines, subscribers)
		self.update_aggregated_data(data['numerator'], data['denominator'])
		return data

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()

	def format_data(self, cards, onlines, subscribers):		
		self.cur.execute(""" SELECT id, name FROM company """)
		brands = self.cur.fetchall()
		numerator 	= {}
		denominator = 0

		for brand in brands:
			numerator[brand[1]] 	= 0

		for brand_count in cards + onlines + subscribers:
			denominator += brand_count[0]
			brand_name = next((item for item in brands if item[0] == brand_count[1]), None)
			if brand_name is None:
				continue
			numerator[brand_name[1]] += brand_count[0]

		return {'numerator': numerator, 'denominator': denominator}


class SalesUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None, last_ts=0):
		super(SalesUpdater, self).__init__()
		self.name 			= 'admin_sales'
		self.split_keys = split_keys
		self.last_ts 		= last_ts

	def calculate_total_data(self):
		processor = AdminSalesEightWeeks('date')	
		processor.run(self.split_keys)
		result = processor.result
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.calculate_total_data()