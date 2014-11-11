#!/usr/bin/env python
# -*- coding: utf-8 -*-
from updater import Updater
from processors.shops_processors import ShopsCustomersByPurchases, ShopsRePurchases, ShopsRePurchasesTotals, CustomersByPurchases, RePurchasesMonth, RePurchasesMonthTotals
from datetime import datetime, timedelta
import time

class CustomersByPurchasesUpdater(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, brand_id, split_keys=None):
		super(CustomersByPurchasesUpdater, self).__init__()
		self.name 			= 'shops_customers_by_purchases_' + str(brand_id)
		self.shop_id 		= brand_id
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = ShopsCustomersByPurchases('card_number', self.shop_id)	
		processor.run(self.split_keys)
		result = processor.result
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE shop_id = %s """, (self.shop_id,))
		customers_count = self.cur.fetchone()[0]
		self.update_aggregated_data(result, customers_count)
		return result

	def calculate_day_data(self, today_ts, yesterday_ts):
		processor = ShopsCustomersByPurchases('card_number', self.shop_id, start_ts=today_ts, end_ts=yesterday_ts)
		processor.run(self.split_keys)
		result = processor.result	
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE date_create >= %s AND date_create < %s AND shop_id = %s """, (yesterday_ts, today_ts, self.shop_id))
		customers_count = self.cur.fetchone()[0]
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += result[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], customers_count + self.aggregated_data['denominator'])
		return self.aggregated_data['numerator']


class FilledProfilesUpdater(Updater):
	""" Доля заполненных анкет """
	def __init__(self, shop_id):
		super(FilledProfilesUpdater, self).__init__()
		self.name 		= 'shop_filled_profiles_' + str(shop_id)
		self.shop_id 	= shop_id
		
	def calculate_total_data(self):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN c.fullness = 2 THEN 1 ELSE 0 END)::FLOAT AS filled, 
			COUNT(c.fullness) as total
			FROM card t JOIN customer c 
			ON t.customer_id = c.id 
			WHERE t.shop_id = %s """, (self.shop_id, ))
		result = self.cur.fetchone()
		numerator 	= 0 if result[0] is None else result[0]
		denominator = 0 if result[1] is None else result[1]
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN c.fullness = 2 THEN 1 ELSE 0 END)::FLOAT AS filled, 
			COUNT(c.fullness) as total
			FROM card t JOIN customer c
			ON t.customer_id = c.id 
			WHERE t.date_create >= %s AND t.date_create < %s AND t.shop_id = %s """, (yesterday_ts, today_ts, self.shop_id))
		result = self.cur.fetchone()
		numerator 	= 0 if result[0] is None else result[0]
		denominator = 0 if result[1] is None else result[1]
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class ParticipantsCountUpdater(Updater):
	""" Число участников """
	def __init__(self, shop_id):
		super(ParticipantsCountUpdater, self).__init__()
		self.name 		= 'shops_participants_count_' + str(shop_id)
		self.shop_id 	= shop_id
		
	def calculate_total_data(self):
		numerator = 0
		self.cur.execute(""" SELECT COUNT(id) FROM card WHERE shop_id = %s """, (self.shop_id, ))
		result = self.cur.fetchone()
		result = 0 if result[0] is None else result[0]
		numerator += result
		denominator = 1
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT COUNT(id) FROM card WHERE date_create >= %s AND date_create < %s AND shop_id = %s""", (yesterday_ts, today_ts, self.shop_id))
		result = self.cur.fetchone()
		numerator = 0 if result[0] is None else result[0]
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class CustomersDataUpdater(Updater):
	""" Соотношение каналов коммуникации и доля согласных на рассылку """
	def __init__(self, shop_id):
		super(CustomersDataUpdater, self).__init__()
		self.name 		= 'shops_customers_data_' + str(shop_id)
		self.shop_id 	= shop_id

	def calculate_total_data(self):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN subscription THEN 1 ELSE 0 END)::FLOAT as subscribed
			FROM card WHERE shop_id = %s """, (self.shop_id, ))
		result = self.cur.fetchone()
		numerator = {
			'all': 				0 if result[0] is None else result[0],
			'email': 			0 if result[1] is None else result[1],
			'sms': 				0 if result[2] is None else result[2],	
			'unknown': 		0 if result[3] is None else result[3],
			'subscribed': 0 if result[4] is None else result[4]
		}
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE shop_id = %s """, (self.shop_id,))
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
			FROM card WHERE date_create >= %s AND date_create < %s AND shop_id = %s """, (yesterday_ts, today_ts, self.shop_id))
		result = self.cur.fetchone()
		numerator = {
			'all': 				0 if result[0] is None else result[0],
			'email': 			0 if result[1] is None else result[1],
			'sms': 				0 if result[2] is None else result[2],	
			'unknown': 		0 if result[3] is None else result[3],
			'subscribed': 0 if result[4] is None else result[4]
		}
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE date_create >= %s AND date_create < %s AND shop_id = %s """, (yesterday_ts, today_ts, self.shop_id))
		denominator = self.cur.fetchone()[0]		
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += numerator[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}


class PurchasesWithCardsUpdater(Updater):
	""" Доля продаж по картам """
	def __init__(self, shop_id, start_ts, end_ts):
		super(PurchasesWithCardsUpdater, self).__init__()
		self.name 		= 'shops_purchases_with_cards_' + str(shop_id)
		self.shop_id 	= shop_id
		self.start_ts = start_ts
		self.end_ts 	= end_ts
		
	def calculate_total_data(self):
		denominator = self.db['dashboard_cheques'].find({
			'shop_id': 	self.shop_id,
			'date': 		{'$gte': self.start_ts, '$lt': self.end_ts}
			}).count()
		numerator 	= self.db['dashboard_cheques'].find({
			'shop_id': 			self.shop_id,
			'date': 				{'$gte': self.start_ts, '$lt': self.end_ts},
			'card_number': 	{'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		denominator = self.db['dashboard_cheques'].find({
			'shop_id': 	self.shop_id,
			'date': 		{'$gte': yesterday_ts, '$lt': today_ts}
			}).count()
		numerator 	= self.db['dashboard_cheques'].find({
			'shop_id': 			self.shop_id,
			'date': 				{'$gte': yesterday_ts, '$lt': today_ts},
			'card_number': 	{'$ne': ''}
			}).count()
		self.update_aggregated_data(numerator + self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class RePurchasesUpdater(Updater):
	""" Доля повторных покупок """
	def __init__(self, shop_id, start_ts=0, name='shops_repurchases', split_keys=None):
		super(RePurchasesUpdater, self).__init__()
		self.name 			= name + '_' + str(shop_id)
		self.shop_id 		= shop_id
		self.start_ts 	= start_ts
		self.split_keys = split_keys

	def calculate_total_data(self):
		processor = ShopsRePurchases('card_number', self.shop_id, self.start_ts)	
		processor.run(self.split_keys)
		numerator = processor.result
		processor = ShopsRePurchasesTotals('card_number', self.shop_id, self.start_ts)	
		processor.run(self.split_keys)
		denominator = processor.result
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		processor = ShopsRePurchases('card_number', self.shop_id, today_ts, yesterday_ts)	
		processor.run(self.split_keys)
		numerator = processor.result
		processor = ShopsRePurchasesTotals('card_number', self.shop_id, today_ts, yesterday_ts)	
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

class GendersDataUpdater(Updater):
	""" Соотношение полов """
	def __init__(self, shop_id):
		super(GendersDataUpdater, self).__init__()
		self.name 		= 'shops_genders_data_' + str(shop_id)
		self.shop_id 	= shop_id

	def calculate_total_data(self):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total FROM personal_data p JOIN customer cst 
			ON cst.digital_id = p.digital_id 
			WHERE cst.id IN (SELECT customer_id FROM card WHERE shop_id = %s)""", (self.shop_id, ))
		result = self.cur.fetchone()
		numerator = {
			'male': 			0 if result[0] is None else result[0],
			'female': 		0 if result[1] is None else result[1],
			'undefined': 	0 if result[2] is None else result[2],	
		}
		denominator 	= 0 if result[3] is None else result[3]
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total FROM personal_data p JOIN customer cst 
			ON cst.digital_id = p.digital_id 
			WHERE cst.id IN (SELECT customer_id FROM card WHERE shop_id = %s) AND cst.date >= %s AND cst.date < %s""", (self.shop_id, yesterday_ts, today_ts))
		result = self.cur.fetchone()
		numerator = {
			'male': 			0 if result[0] is None else result[0],
			'female': 		0 if result[1] is None else result[1],
			'undefined': 	0 if result[2] is None else result[2],	
		}
		denominator 	= 0 if result[3] is None else result[3]
		for k in self.aggregated_data['numerator']:
			try:
				self.aggregated_data['numerator'][k] += numerator[k]
			except KeyError:
				pass
		self.update_aggregated_data(self.aggregated_data['numerator'], denominator + self.aggregated_data['denominator'])
		return {'numerator': self.aggregated_data['numerator'], 'denominator': self.aggregated_data['denominator']}
		

class CardsAddedUpdater(Updater):
	""" Доля выданных бонусных карт """
	def __init__(self, shop_id, start_ts, name='shops_cards_added'):
		super(CardsAddedUpdater, self).__init__()
		self.name 		= name + '_' + str(shop_id)
		self.shop_id 	= shop_id
		self.start_ts = start_ts
		
	def calculate_total_data(self):
		self.cur.execute(""" SELECT COUNT('id') FROM card WHERE date_create >= %s AND shop_id = %s """, (self.start_ts, self.shop_id))
		numerator = self.cur.fetchone()[0]

		denominator = self.db['dashboard_cheques'].find({
			'shop_id': 	self.shop_id,
			'date': 		{'$gte': self.start_ts}
			}).count()
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts=0, yesterday_ts=0):
		self.calculate_total_data()

		
class SalesEightWeeksUpdater(Updater):
	""" Продажи за последние 8 недель """
	def __init__(self, shop_id):
		super(SalesEightWeeksUpdater, self).__init__()
		self.name 		= 'shops_eight_weeks_sales_' + str(shop_id)
		self.shop_id 	= shop_id
		
	def calculate_total_data(self):		
		today 				= datetime.today().replace(hour=0, minute=0, second=0)
		week_start 		= today - timedelta(days=today.weekday(), weeks=7)
		week_start_ts = int(time.mktime(week_start.timetuple()))
		numerator 		= []
		for w in range(8):
			start 		= today - timedelta(days=today.weekday(), weeks=w)
			start_ts 	= int(time.mktime(start.timetuple()))
			end 			= today - timedelta(days=today.weekday(), weeks=w-1)
			end_ts 		= int(time.mktime(end.timetuple()))
			pipeline 	= [
				{
					'$match': {
						'$and': [{'date': {'$gt': start_ts}}, {'date': {'$lte': end_ts}}, {'shop_id': self.shop_id}]
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
		denominator = 1
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts=0, yesterday_ts=0):
		self.calculate_total_data()
		

class BonusesUpdater(Updater):
	""" Доли начислений и списаний """
	def __init__(self, shop_id, start_ts):
		super(BonusesUpdater, self).__init__()
		self.name 		= 'shops_bonuses_' + str(shop_id)
		self.shop_id 	= shop_id
		self.start_ts = start_ts
		
	def calculate_total_data(self):
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': 		{'$gte': self.start_ts},
					'shop_id': 	{'$eq': self.sh}
				}
			},
			{
				'$group': {
					'_id': 			None,
					'accrual': 	{'$sum': '$accrual'},
					'used': 		{'$sum': '$used'},
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
					'date': 		{'$gte': self.start_ts},
					'shop_id': 	{'$eq': self.shop_id}
				}
			},
			{
				'$group': {
					'_id': 	None,
					'sum': 	{'$sum': '$sum'}
				}
			}
			])
		denominator = result['result'][0]['sum']
		self.update_aggregated_data(numerator, denominator)
		return {'numerator': numerator, 'denominator': denominator}

	def calculate_day_data(self, today_ts, yesterday_ts):
		result = self.db['dashboard_cheques'].aggregate([
			{
				'$match': {
					'date': 		{'$gte': yesterday_ts},
					'brand_id': {'$eq': self.shop_id}
				}
			},
			{
				'$group': {
					'_id': 			None,
					'accrual': 	{'$sum': '$accrual'},
					'used': 		{'$sum': '$used'},
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
					'date': 		{'$gte': yesterday_ts},
					'shop_id': 	{'$eq': self.shop_id}
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
		

class CustomersByPurchasesUpdaterNew(Updater):
	""" Доли клиентов с определенным числом покупок """
	def __init__(self, split_keys=None):
		super(CustomersByPurchasesUpdaterNew, self).__init__()
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