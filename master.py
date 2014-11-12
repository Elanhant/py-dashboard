#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, json
import psycopg2
import psycopg2.extras
import updaters.admin_updaters as admin_updaters
import updaters.brands_updaters as brands_updaters
import updaters.shops_updaters as shops_updaters
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta
from config import *

current_milli_time = lambda: int(round(time.time() * 1000))

class DashboardMaster(object):
	client 				= MongoClient(MONGO_CLIENT_HOST)
	db_name 			= MONGO_DB_NAME
	postgres_conn = psycopg2.connect(POSTGRESQL_CONN_STRING)

	@staticmethod
	def find_shop_data(shop_id, haystack):
		return next((item for item in haystack if item["shop_id"] == shop_id), None)

	@staticmethod
	def find_brand_data(brand_id, haystack):
		return next((item for item in haystack if item["company_id"] == brand_id), None)

	def __init__(self):
		self.cur 						= self.postgres_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		self.today 					= datetime.today().replace(hour=0, minute=0, second=0)
		self.today_ts 			= int(time.mktime(self.today.timetuple()))
		self.yesterday 			= self.today - timedelta(days=1)
		self.yesterday_ts 	= int(time.mktime(self.yesterday.timetuple()))
		self.week_start 		= self.today - timedelta(days=self.today.weekday())
		self.week_start_ts 	= int(time.mktime(self.week_start.timetuple()))
		self.month_start 		= datetime(datetime.today().year, datetime.today().month, 1)
		self.month_start_ts = int(time.mktime(self.month_start.timetuple()))
		self.year_start 		= datetime(datetime.today().year, 1, 1)
		self.year_start_ts 	= int(time.mktime(self.year_start.timetuple()))

		self.splitted_keys = None

		self.brands_cards_added					= []
		self.brands_customers_data			= []
		self.brands_filled_profiles			= []
		self.brands_genders_data				= []
		self.brands_participants_counts = []

		self.shops_cards_added 					= []
		self.shops_customers_data 			= []
		self.shops_filled_profiles 			= []
		self.shops_genders_data 				= []
		self.shops_participants_counts 	= []


	def run(self):		
		run_start = current_milli_time()	
		print "Dashboard Updater Started at %s" % time.ctime()

		# Заполняем (или обновляем) собственную таблицу чеков
		if self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].count() == 0 or FORCE_REFILL is True:
			self.fill_cheques()
		else:
			self.update_cheques()
		print "Took %s ms to fill cheques" % (current_milli_time() - run_start,)

		# Разбиваем коллекцию на блоки
		t_start = current_milli_time()	
		self.split_keys()
		print "Took %s ms to split keys" % (current_milli_time() - t_start,)

		# При необходимости удаляем накопленные за прошлые запуски данные
		if CLEAR_AGGREGATED_DATA is True:
			self.client[self.db_name][UPDATER_COLLECTION].remove()

		# Получаем данные из Postgresql-базы, сгрупированные по ID магазина
		self.get_shops_postgre_data()

		# Получаем данные из Postgresql-базы, сгрупированные по ID бренда
		self.get_brands_postgre_data()

		# Вычисляем и сохраняем в отдельные коллекции продажи за 8 недель, а также 
		# доли повторных покупок и доли клиентов по числу покупок
		self.run_aggregators()

		# Вычисление дэшборда для администраторов
		t_start = current_milli_time()	
		self.calculate_admin_dashboard()
		print "Took %s ms to calculate admin dashboard" % (current_milli_time() - t_start,)

		self.cur.execute(""" SELECT id FROM company""")
		brands = self.cur.fetchall()
		for brand_tuple in brands:
			t_start = current_milli_time()	
			self.calculate_brand_dashboard(brand_tuple[0])
			print "Took %s ms to calculate dashboard for brand %s" % (current_milli_time() - t_start, brand_tuple[0])
			self.cur.execute(""" SELECT id FROM shop WHERE company_id = %s """, (brand_tuple[0], ))
			shops = self.cur.fetchall()
			for shop_tuple in shops:
				t_start = current_milli_time()	
				self.calculate_shop_dashboard(shop_tuple[0], brand_tuple[0])
				# print "Took %s ms to calculate dashboard for shop %s" % (current_milli_time() - t_start, shop_tuple[0])

		if CLEAR_TEMPORARY_COLLECTIONS is True:
			for c in [DASHBOARD_CHEQUES_MONTH_COLLECTION, AGGREGATED_CARDS_BY_PURCHASES_SHOPS, AGGREGATED_CARDS_BY_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_SHOPS, AGGREGATED_RE_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_TOTALS_SHOPS, AGGREGATED_RE_PURCHASES_TOTALS_BRANDS]:
				self.client[self.db_name][c].drop()
			for w in range(8):
				self.client[self.db_name][WEEKS_SALES_COLLECTION_PREFIX + str(w)].drop()

		print "The time is %s, took %s ms to complete" % (time.ctime(), current_milli_time() - run_start)


	def get_shops_postgre_data(self):
		t_start = current_milli_time()
		""" Количество добавленнных карт """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as month,
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as week,
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as day,
			shop_id
			FROM card
			GROUP BY shop_id """, (self.month_start_ts, self.week_start_ts, self.yesterday_ts))
		self.shops_cards_added = self.cur.fetchall()

		""" Доли каналов коммуникации и согласных на рассылку """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN subscription THEN 1 ELSE 0 END)::FLOAT as subscribed,
			COUNT(id) as total,
			shop_id
			FROM card GROUP BY shop_id """)
		self.shops_customers_data = self.cur.fetchall()

		""" Доля заполненных анкет """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN c.fullness = 2 THEN 1 ELSE 0 END)::FLOAT / COUNT(c.fullness) AS filled, 
			t.shop_id
			FROM card t JOIN customer c 
			ON t.customer_id = c.id 
			GROUP BY t.shop_id """)
		self.shops_filled_profiles = self.cur.fetchall()

		""" Соотношение полов """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total,
			c.shop_id as shop_id
			FROM personal_data p 
			JOIN customer cst ON cst.digital_id = p.digital_id 
			JOIN card c ON c.customer_id = cst.id
			GROUP BY c.shop_id""")
		self.shops_genders_data = self.cur.fetchall()

		""" Количество участников """
		self.cur.execute(""" SELECT COUNT(id), shop_id FROM card GROUP BY shop_id """)
		self.shops_participants_counts = self.cur.fetchall()
		print "Took %s ms to get shops data from POSTGRESQL" % (current_milli_time() - t_start, )


	def get_brands_postgre_data(self):
		t_start = current_milli_time()
		""" Количество добавленнных карт """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as month,
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as week,
			SUM(CASE WHEN date_create >= %s THEN 1 ELSE 0 END) as day,
			company_id
			FROM card
			GROUP BY company_id """, (self.month_start_ts, self.week_start_ts, self.yesterday_ts))
		self.brands_cards_added = self.cur.fetchall()

		""" Доли каналов коммуникации и согласных на рассылку """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN subscription THEN 1 ELSE 0 END)::FLOAT as subscribed,
			COUNT(id) as total,
			company_id
			FROM card GROUP BY company_id """)
		self.brands_customers_data = self.cur.fetchall()

		""" Доля заполненных анкет """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN c.fullness = 2 THEN 1 ELSE 0 END)::FLOAT / COUNT(c.fullness) AS filled, 
			t.company_id
			FROM card t JOIN customer c 
			ON t.customer_id = c.id 
			GROUP BY t.company_id """)
		self.brands_filled_profiles = self.cur.fetchall()

		""" Соотношение полов """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total,
			c.company_id as company_id
			FROM personal_data p 
			JOIN customer cst ON cst.digital_id = p.digital_id 
			JOIN card c ON c.customer_id = cst.id
			GROUP BY c.company_id""")
		self.brands_genders_data = self.cur.fetchall()

		""" Количество участников """
		self.cur.execute(""" SELECT COUNT(id), company_id FROM card GROUP BY company_id """)
		self.brands_participants_counts = self.cur.fetchall()
		print "Took %s ms to get brands data from POSTGRESQL" % (current_milli_time() - t_start, )


	def run_aggregators(self):
		""" Продажи за 8 недель """
		sales_updater = admin_updaters.SalesUpdater()
		sales_updater.run()

		""" Доли клиентов с определенным числом покупок (для магазинов) """
		customers_by_purchases_updater = shops_updaters.CustomersByPurchasesUpdaterNew(split_keys=self.splitted_keys)
		customers_by_purchases_updater.run()

		""" Доли клиентов с определенным числом покупок (для брендов) """
		customers_by_purchases_updater = brands_updaters.CustomersByPurchasesUpdaterNew(split_keys=self.splitted_keys)
		customers_by_purchases_updater.run()

		""" Доли повторных покупок (для магазинов) """
		re_purchases_updater = shops_updaters.RePurchasesMonthUpdater(split_keys=self.splitted_keys)
		re_purchases_updater.run()

		re_purchases_totals_updater = shops_updaters.RePurchasesMonthTotalsUpdater(split_keys=self.splitted_keys)
		re_purchases_totals_updater.run()

		""" Доли повторных покупок (для брендов) """
		re_purchases_updater = brands_updaters.RePurchasesMonthUpdater(split_keys=self.splitted_keys)
		re_purchases_updater.run()

		re_purchases_totals_updater = brands_updaters.RePurchasesMonthTotalsUpdater(split_keys=self.splitted_keys)
		re_purchases_totals_updater.run()


	def fill_cheques(self):
		self.client[self.db_name].command('aggregate', CHEQUES_COLLECTION,
			pipeline=[
				{
					'$project': {
						'card_number': 	1,
						'sum': 					1,
						'date': 				1,
						# 'brand': 				1,
						'brand_id': 		1,
						# 'shop_code': 		1,
						'shop_id': 			1,
						'accrual': 			1,
						'used': 				1,
						'document_number': {
							'$cond': [{'$gt': [{'$cmp': ['$document_number_sale', '']}, 0]}, '$document_number_sale', '$document_number']
						},
						'return_sum': {
							'$cond': [{'$gt': [{'$cmp': ['$document_number_sale', '']}, 0]}, '$sum', 0]
						}
					}
				},
				{
					'$group': {
						'_id': 							'$document_number',
						'card_number': 			{'$first': '$card_number'},
						'document_number': 	{'$first': '$document_number'},
						'sum': 							{'$max': '$sum'},
						'return_sum': 			{'$max': '$return_sum'},
						'accrual': 					{'$max': '$accrual'},
						'used': 						{'$max': '$used'},
						# 'brand': 						{'$first': '$brand'},
						'brand_id': 				{'$first': '$brand_id'},
						# 'shop_code': 				{'$first': '$shop_code'},
						'shop_id': 					{'$first': '$shop_id'},
						'date': 						{'$first': '$date'},
					}
				},
				{
					'$out': DASHBOARD_CHEQUES_COLLECTION
				}
			], allowDiskUse=True
			)
		self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].ensure_index([('card_number', ASCENDING)])
		self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].ensure_index([('date', ASCENDING)])


	def update_cheques(self, days=30):
		start 		= self.today - timedelta(days=days)
		start_ts 	= int(time.mktime(start.timetuple()))
		self.client[self.db_name].command('aggregate', CHEQUES_COLLECTION,
			pipeline=[
				{
					'$match': {'date': {'$gte': start_ts}}
				},
				{
					'$sort': {'date': 1}
				},
				{
					'$project': {
						'card_number': 	1,
						'sum': 					1,
						'date': 				1,
						# 'brand': 				1,
						'brand_id': 		1,
						# 'shop_code': 		1,
						'shop_id': 			1,
						'accrual': 			1,
						'used': 				1,
						'document_number': {
							'$cond': [{'$gt': [{'$cmp': ['$document_number_sale', '']}, 0]}, '$document_number_sale', '$document_number']
						},
						'return_sum': {
							'$cond': [{'$gt': [{'$cmp': ['$document_number_sale', '']}, 0]}, '$sum', 0]
						}
					}
				},
				{
					'$group': {
						'_id': 							'$document_number',
						'card_number': 			{'$first': '$card_number'},
						'document_number': 	{'$first': '$document_number'},
						'sum': 							{'$max': '$sum'},
						'return_sum': 			{'$max': '$return_sum'},
						'accrual': 					{'$max': '$accrual'},
						'used': 						{'$max': '$used'},
						# 'brand': 						{'$first': '$brand'},
						'brand_id': 				{'$first': '$brand_id'},
						# 'shop_code': 				{'$first': '$shop_code'},
						'shop_id': 					{'$first': '$shop_id'},
						'date': 						{'$first': '$date'},
					}
				},
				{
					'$out': DASHBOARD_CHEQUES_MONTH_COLLECTION
				}
			], allowDiskUse=True
			)
		cursor = self.client[self.db_name][DASHBOARD_CHEQUES_MONTH_COLLECTION].find()
		for cheque in cursor:
			self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].update({'_id': cheque['_id']}, cheque, upsert=True)
		self.client[self.db_name][DASHBOARD_CHEQUES_MONTH_COLLECTION].drop()


	def split_keys(self, key='card_number'):
		self.splitted_keys = self.client[self.db_name].command(
			"splitVector", self.db_name + "." + DASHBOARD_CHEQUES_COLLECTION,
			keyPattern={key : 1}, 
			maxChunkSizeBytes=32000
		)['splitKeys']


	def calculate_admin_dashboard(self):
		dashboard_data = {
			'brand_id': 0,
			'type'		: 'admin'
		}

		""" Доля повторных покупок с начала года """
		repurchases_updater = admin_updaters.RePurchasesUpdater(self.year_start_ts, 'admin_repurchases_year', split_keys=self.splitted_keys)
		repurchases_updater.run()

		aggregated 	= repurchases_updater.find_aggregated_data()	
		totals 			= sum(aggregated['denominator'].itervalues())
		percent 		= 0 if totals == 0 else 100.0 * (aggregated['denominator']['has_card'] - aggregated['numerator']['not_firsts']) / totals
		dashboard_data['re_purchases_year'] = percent


		""" Соотношение каналов коммуникации и доля согласных на рассылку """
		customers_data_updater = admin_updaters.CustomersDataUpdater()
		customers_data_updater.run()

		aggregated 	= customers_data_updater.find_aggregated_data()
		percents 		= {
			'channels': {
				'all': 			0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['all'] / aggregated['denominator'],
				'email': 		0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['email'] / aggregated['denominator'],
				'sms': 			0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['sms'] / aggregated['denominator'],
				'unknown': 	0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['unknown'] / aggregated['denominator'],
			},
			'subscribed': 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['subscribed'] / aggregated['denominator'],
		}
		dashboard_data['channels_data'] = percents['channels']
		dashboard_data['subscribed'] 		= percents['subscribed']


		""" Число участников """
		participants_count_updater = admin_updaters.ParticipantsCountUpdater()
		participants_count_updater.run()
		
		aggregated = participants_count_updater.find_aggregated_data()	
		dashboard_data['participants_count'] = aggregated['numerator']


		""" Доли клиентов с определенным числом покупок """
		customers_by_purchases_updater = admin_updaters.CustomersByPurchasesUpdater(split_keys=self.splitted_keys)
		customers_by_purchases_updater.run()

		aggregated = customers_by_purchases_updater.find_aggregated_data()
		cards_with_cheques = 0
		percents = {
			'qty_0': {
				'label': 'Клиентов без покупок',
				'value': 0
			},
			'qty_1': {
				'label': 'Клиентов с 1 покупкой',
				'value': 0
			},
			'qty_2': {
				'label': 'Клиентов с 2 покупками',
				'value': 0
			},
			'qty_3': {
				'label': 'Клиентов с 3 и более покупками',
				'value': 0
			},
		}
		for x in aggregated['numerator']:
			cards_with_cheques += aggregated['numerator'][x]
			percents['qty_' + str(x)]['value'] = 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'][x] / aggregated['denominator']
		no_cheques = aggregated['denominator'] - cards_with_cheques
		percents['qty_0']['value'] = 0 if aggregated['denominator'] == 0 else 100.0 * no_cheques / aggregated['denominator']
		dashboard_data['progress_data'] = percents


		""" Доля повторных покупок с начала месяца """
		repurchases_updater = admin_updaters.RePurchasesUpdater(self.month_start_ts, 'admin_repurchases_month', split_keys=self.splitted_keys)
		repurchases_updater.run()

		aggregated 	= repurchases_updater.find_aggregated_data()	
		totals 			= sum(aggregated['denominator'].itervalues())
		percent 		= 0 if totals == 0 else 100.0 * (aggregated['denominator']['has_card'] - aggregated['numerator']['not_firsts']) / totals
		dashboard_data['re_purchases_month'] = percent


		""" Соотношение полов """
		genders_data_updater = admin_updaters.GendersDataUpdater()
		genders_data_updater.run()
		
		aggregated = genders_data_updater.find_aggregated_data()	
		percents = {
			'male': 			0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['male'] / aggregated['denominator'],
			'female': 		0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['female'] / aggregated['denominator'],
			'undefined': 	0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['undefined'] / aggregated['denominator'],
		}
		dashboard_data['genders_data'] = percents


		""" Клиентская база """
		participants_count_updater = admin_updaters.ParticipantsCountUpdater()
		participants_count_updater.run()

		aggregated = participants_count_updater.find_aggregated_data()
		dashboard_data['participants_count'] = aggregated['numerator']


		""" Доли списаний и начислений """
		bonuses_updater = admin_updaters.BonusesUpdater(self.month_start_ts)
		bonuses_updater.run()
		
		aggregated 	= bonuses_updater.find_aggregated_data()	
		percents 		= {
			'accrual': 	0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['accrual'] / aggregated['denominator'],
			'used': 		0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['used'] / aggregated['denominator'],
		}
		dashboard_data['bonuses_data'] = percents


		""" Доля продаж по картам с начала месяца """
		purchases_with_cards_updater = admin_updaters.PurchasesWithCardsUpdater(self.month_start_ts, self.today_ts)
		purchases_with_cards_updater.run()

		aggregated 	= purchases_with_cards_updater.find_aggregated_data()	
		percent 		= 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'] / aggregated['denominator']
		dashboard_data['purchased_with_cards'] = percent


		""" Продажи за последние 8 недель """
		data 			= []
		no_sales 	= {'has_card': 0, 'no_card': 0, 'overall': 0}
		for w in range(8):
			start 		= self.today - timedelta(days=self.today.weekday(), weeks=w)
			week_data = self.client[self.db_name][WEEKS_SALES_COLLECTION_PREFIX + str(w)].aggregate([
				{			
			    '$group': {
			      '_id': 			None,
		        'has_card': {'$sum': '$has_card'},
		        'no_card': 	{'$sum': '$no_card'},
		        'overall': 	{'$sum': '$overall'},
			    } 
		    }, 
		    {
			    '$project': {
		    		'has_card': 1, 
		    		'no_card': 	1, 
		    		'overall': 	1,
						'label':  	{'$literal': 'Неделя ' + str(start.isocalendar()[1])}
			    }
				}
			])
			data.append(no_sales if not week_data['result'] else week_data['result'][0])
		data.reverse()
		dashboard_data['sales_data'] = data


		""" Число участников в разрезе брендов """
		participants_by_brands_updater = admin_updaters.ParticipantsByBrandsUpdater()
		participants_by_brands_updater.run()

		aggregated 	= participants_by_brands_updater.find_aggregated_data()
		percents 		= {}
		for brand_name in aggregated['numerator']:
			percents[brand_name] = 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'][brand_name] / aggregated['denominator']
		dashboard_data['participants_by_brands'] = percents

		self.update_dashboard({"brand_id": 0}, dashboard_data)


	def calculate_brand_dashboard(self, brand_id):
		dashboard_data = {
			'brand_id': brand_id,
			'type'		: 'operator'
		}

		""" Соотношение каналов коммуникации и доля согласных на рассылку """
		data = self.find_brand_data(brand_id, self.brands_customers_data) or {'total': 0, 'email': 0, 'sms': 0, 'unknown': 0, 'subscribed': 0}
		percents = {
			'channels': {
				'all': 			0 if data['total'] == 0 else 100.0 * data['all'] / data['total'],
				'email': 		0 if data['total'] == 0 else 100.0 * data['email'] / data['total'],
				'sms': 			0 if data['total'] == 0 else 100.0 * data['sms'] / data['total'],
				'unknown': 	0 if data['total'] == 0 else 100.0 * data['unknown'] / data['total'],
			},
			'subscribed': 0 if data['total'] == 0 else 100.0 * data['subscribed'] / data['total'],
		}
		dashboard_data['channels_data'] = percents['channels']
		dashboard_data['subscribed'] = percents['subscribed']


		""" Доли клиентов с определенным числом покупок """
		aggregated = self.client[self.db_name][AGGREGATED_CARDS_BY_PURCHASES_BRANDS].find_one({'brand_id': brand_id})
		cards_with_cheques = 0
		percents = {
			'qty_0': {
				'label': 'Клиентов без покупок',
				'value': 0
			},
			'qty_1': {
				'label': 'Клиентов с 1 покупкой',
				'value': 0
			},
			'qty_2': {
				'label': 'Клиентов с 2 покупками',
				'value': 0
			},
			'qty_3': {
				'label': 'Клиентов с 3 и более покупками',
				'value': 0
			},
		}
		if aggregated is not None:
			denominator = self.find_brand_data(brand_id, self.brands_participants_counts) or {'count': 0}
			for key in ['qty_1', 'qty_2', 'qty_3']:
				cards_with_cheques += aggregated[key]
				percents[key]['value'] = 0 if denominator['count'] == 0 else 100.0 * aggregated[key] / denominator['count']
			no_cheques = denominator['count'] - cards_with_cheques
			percents['qty_0']['value'] = 0 if denominator['count'] == 0 else 100.0 * no_cheques / denominator['count']
		dashboard_data['progress_data'] = percents


		""" Доля повторных покупок с начала месяца """
		not_firsts 	= self.client[self.db_name][AGGREGATED_RE_PURCHASES_BRANDS].find_one({'brand_id': brand_id})	
		totals_data = self.client[self.db_name][AGGREGATED_RE_PURCHASES_TOTALS_BRANDS].find_one({'brand_id': brand_id})	
		total_sum 	= 0 if totals_data is None else totals_data['has_card'] + totals_data['no_card']
		percent 		= 0 if total_sum == 0 else 100.0 * (totals_data['has_card'] - not_firsts['not_firsts']) / total_sum
		dashboard_data['re_purchases_month'] = percent


		""" Доля заполненных анкет """
		data = self.find_brand_data(brand_id, self.brands_filled_profiles) or {'filled': 0}
		dashboard_data['filled_profiles'] = percent


		""" Доля продаж по картам с начала месяца """
		purchases_with_cards_updater = brands_updaters.PurchasesWithCardsUpdater(brand_id, self.month_start_ts, self.today_ts)
		purchases_with_cards_updater.run()

		aggregated 	= purchases_with_cards_updater.find_aggregated_data()	
		percent 		= 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'] / aggregated['denominator']
		dashboard_data['purchased_with_cards'] = percent


		""" Число участников """
		data = self.find_brand_data(brand_id, self.brands_participants_counts) or {'count': 0}
		dashboard_data['participants_count'] = data['count']


		""" Доля выданных бонусных карт с начала месяца """
		data = self.find_brand_data(brand_id, self.brands_cards_added) or {'month': 0, 'week': 0, 'day': 0}
		dashboard_data['cards_added_month'] = data['month']
		dashboard_data['cards_added_week'] 	= data['week']
		dashboard_data['cards_added_day'] 	= data['day']


		""" Соотношение полов """
		data = self.find_brand_data(brand_id, self.brands_genders_data) or {'total': 0, 'email': 0, 'sms': 0, 'unknown': 0, 'subscribed': 0}
		percents = {
				'male': 			0 if data['total'] == 0 else 100.0 * data['male'] / data['total'],
				'female': 		0 if data['total'] == 0 else 100.0 * data['female'] / data['total'],
				'undefined': 	0 if data['total'] == 0 else 100.0 * data['undefined'] / data['total'],
		}
		dashboard_data['genders_data'] = percents


		""" Продажи за последние 8 недель """
		data 			= []
		no_sales 	= {'has_card': 0, 'no_card': 0, 'overall': 0}
		for w in range(8):
			start 		= self.today - timedelta(days=self.today.weekday(), weeks=w)
			week_data = self.client[self.db_name][WEEKS_SALES_COLLECTION_PREFIX + str(w)].aggregate([
				{
					'$match': {'_id.brand_id': brand_id}
				},
				{			
			    '$group': {
			        '_id': 			'$_id.brand_id',
			        'has_card': {'$sum': '$has_card'},
			        'no_card': 	{'$sum': '$no_card'},
			        'overall': 	{'$sum': '$overall'},
			    }
				}, 
		    {
			    '$project': {
		    		'has_card': 1, 
		    		'no_card': 	1, 
		    		'overall': 	1,
						'label':  	{'$literal': 'Неделя ' + str(start.isocalendar()[1])}
			    }
				}
			])
			data.append(no_sales if not week_data['result'] else week_data['result'][0])
		data.reverse()
		dashboard_data['sales_data'] = data


		""" Доли списаний и начислений """
		bonuses_updater = brands_updaters.BonusesUpdater(brand_id, self.month_start_ts)
		bonuses_updater.run()

		aggregated 	= bonuses_updater.find_aggregated_data()	
		percents 		= {
			'accrual': 	0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['accrual'] / aggregated['denominator'],
			'used': 		0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator']['used'] / aggregated['denominator'],
		}
		dashboard_data['bonuses_data'] = percents
		

		""" Число покупателей за позавчерашний день """
		last_day_start 		= self.today - timedelta(days=2)
		last_day_start_ts = int(time.mktime(last_day_start.timetuple()))
		data = self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].find({
			'brand_id': brand_id, 
			'date': {
				'$gte': last_day_start_ts,
				'$lte': self.yesterday_ts
			}
			}).count()
		dashboard_data['last_day_customers'] = 0 if not data else data


		self.update_dashboard({"brand_id": brand_id}, dashboard_data)


	def calculate_shop_dashboard(self, shop_id, brand_id):
		dashboard_data = {
			'brand_id': brand_id,
			'shop_id'	: shop_id,
			'type'		: 'seller'
		}

		""" Соотношение каналов коммуникации и доля согласных на рассылку """
		data 			= self.find_shop_data(shop_id, self.shops_customers_data) or {'total': 0, 'email': 0, 'sms': 0, 'unknown': 0, 'subscribed': 0}
		percents 	= {
			'channels': {
				'all': 			0 if data['total'] == 0 else 100.0 * data['all'] / data['total'],
				'email': 		0 if data['total'] == 0 else 100.0 * data['email'] / data['total'],
				'sms': 			0 if data['total'] == 0 else 100.0 * data['sms'] / data['total'],
				'unknown': 	0 if data['total'] == 0 else 100.0 * data['unknown'] / data['total'],
			},
			'subscribed': 0 if data['total'] == 0 else 100.0 * data['subscribed'] / data['total'],
		}
		dashboard_data['channels_data'] = percents['channels']
		dashboard_data['subscribed'] 		= percents['subscribed']


		""" Доли клиентов с определенным числом покупок """
		aggregated = self.client[self.db_name][AGGREGATED_CARDS_BY_PURCHASES_SHOPS].find_one({'shop_id': shop_id})
		cards_with_cheques = 0
		percents = {
			'qty_0': {
				'label': 'Клиентов без покупок',
				'value': 0
			},
			'qty_1': {
				'label': 'Клиентов с 1 покупкой',
				'value': 0
			},
			'qty_2': {
				'label': 'Клиентов с 2 покупками',
				'value': 0
			},
			'qty_3': {
				'label': 'Клиентов с 3 и более покупками',
				'value': 0
			},
		}
		if aggregated is not None:
			denominator = self.find_shop_data(shop_id, self.shops_participants_counts) or {'count': 0}
			for key in ['qty_1', 'qty_2', 'qty_3']:
				cards_with_cheques += aggregated[key]
				percents[key]['value'] = 0 if denominator['count'] == 0 else 100.0 * aggregated[key] / denominator['count']
			no_cheques = denominator['count'] - cards_with_cheques
			percents['qty_0']['value'] = 0 if denominator['count'] == 0 else 100.0 * no_cheques / denominator['count']

		dashboard_data['progress_data'] = percents


		""" Доля повторных покупок с начала месяца """
		not_firsts 	= self.client[self.db_name][AGGREGATED_RE_PURCHASES_SHOPS].find_one({'shop_id': shop_id})	
		totals_data = self.client[self.db_name][AGGREGATED_RE_PURCHASES_TOTALS_SHOPS].find_one({'shop_id': shop_id})	
		total_sum 	= 0 if totals_data is None else totals_data['has_card'] + totals_data['no_card']
		percent 		= 0 if total_sum == 0 else 100.0 * (totals_data['has_card'] - not_firsts['not_firsts']) / total_sum
		dashboard_data['re_purchases_month'] = percent


		""" Доля заполненных анкет """
		data = self.find_shop_data(shop_id, self.shops_filled_profiles) or {'filled': 0}
		dashboard_data['filled_profiles'] = data['filled']


		""" Доля продаж по картам с начала месяца """
		purchases_with_cards_updater = shops_updaters.PurchasesWithCardsUpdater(brand_id, self.month_start_ts, self.today_ts)
		purchases_with_cards_updater.run()

		aggregated 	= purchases_with_cards_updater.find_aggregated_data()	
		percent 		= 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'] / aggregated['denominator']
		dashboard_data['purchased_with_cards'] = percent


		""" Число участников """
		data = self.find_shop_data(shop_id, self.shops_participants_counts) or {'count': 0}
		dashboard_data['participants_count'] = data['count']


		""" Доли выданных бонусных карт с начала месяца, недели и вчерашнего дня """
		data = self.find_shop_data(shop_id, self.shops_cards_added) or {'month': 0, 'week': 0, 'day': 0}
		dashboard_data['cards_added_month'] = data['month']
		dashboard_data['cards_added_week'] 	= data['week']
		dashboard_data['cards_added_day'] 	= data['day']


		""" Соотношение полов """
		data = self.find_shop_data(shop_id, self.shops_genders_data) or {'total': 0, 'email': 0, 'sms': 0, 'unknown': 0, 'subscribed': 0}
		percents = {
				'male': 			0 if data['total'] == 0 else 100.0 * data['male'] / data['total'],
				'female': 		0 if data['total'] == 0 else 100.0 * data['female'] / data['total'],
				'undefined': 	0 if data['total'] == 0 else 100.0 * data['undefined'] / data['total'],
		}
		dashboard_data['genders_data'] = percents


		""" Продажи за последние 8 недель """
		data = []
		no_sales = {'has_card': 0, 'no_card': 0, 'overall': 0}
		for w in range(8):
			start 		= self.today - timedelta(days=self.today.weekday(), weeks=w)
			week_data = self.client[self.db_name][WEEKS_SALES_COLLECTION_PREFIX + str(w)].find_one({'_id.shop_id': shop_id})
			week_data = no_sales if not week_data else week_data
			week_data['label'] = 'Неделя ' + str(start.isocalendar()[1])
			data.append(week_data)
		data.reverse()
		dashboard_data['sales_data'] = data


		""" Число покупателей за позавчерашний день """
		last_day_start 		= self.today - timedelta(days=2)
		last_day_start_ts = int(time.mktime(last_day_start.timetuple()))
		data = self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].find({
			'shop_id': shop_id, 
			'date': {
				'$gte': last_day_start_ts,
				'$lte': self.yesterday_ts
			}
			}).count()
		dashboard_data['last_day_customers'] = 0 if not data else data


		self.update_dashboard({"brand_id": brand_id, "shop_id": shop_id}, dashboard_data)


	def update_dashboard(self, query, data, upsert=True):
		new_data = {'data': data}
		new_data.update(query)
		self.client[self.db_name][DASHBOARD_COLLECTION].update(query, new_data, upsert=upsert)