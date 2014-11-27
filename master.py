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

	def __init__(self, log=None):
		self.log 						= log
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

		self.brands_groups = []

		self.cards_data 								= []
		self.genders_data								= []
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


	def print_message(self, msg, to_console=True):
		if to_console is True or self.log is None:
			print msg
		if self.log is not None:
			self.log.write(msg + '\n')


	def run(self):		
		run_start = current_milli_time()	
		self.print_message("Dashboard Updater Started at %s, now filling cheques..." % time.ctime())

		# Заполняем (или обновляем) собственную таблицу чеков
		if self.client[self.db_name][DASHBOARD_CHEQUES_COLLECTION].count() == 0 or FORCE_REFILL is True:
			self.fill_cheques()
		else:
			self.update_cheques()
		self.print_message("Took %s ms to fill cheques, now splitting keys..." % (current_milli_time() - run_start,))

		# Разбиваем коллекцию на блоки
		t_start = current_milli_time()	
		self.split_keys()
		self.print_message("Took %s ms to split keys, now getting POSTGRESQL data..." % (current_milli_time() - t_start,))

		# При необходимости удаляем накопленные за прошлые запуски данные
		if CLEAR_AGGREGATED_DATA is True:
			self.client[self.db_name][UPDATER_COLLECTION].remove()

		# Получаем данные из Postgresql-базы, сгрупированные по ID магазина
		self.get_shops_postgre_data()

		# Получаем данные из Postgresql-базы, сгрупированные по ID бренда
		self.get_brands_postgre_data()

		# Вычисляем и сохраняем в отдельные коллекции продажи за 8 недель, а также 
		# доли повторных покупок и доли клиентов по числу покупок
		t_start = current_milli_time()
		self.run_aggregators()
		self.print_message("Took %s ms to aggregate mongo data, now calculating admin dashboard..." % (current_milli_time() - t_start,))

		# Делаем выборку с ID несвязанных брендов
		self.cur.execute(""" SELECT id, company_group_id FROM company""")
		brands = self.cur.fetchall()
		seen_groups = {}
		for brand_tuple in brands:
			if brand_tuple[1] == 0:
				self.brands_groups.append(brand_tuple[0])
			else:
				seen_groups[brand_tuple[1]] = brand_tuple[0]
		self.brands_groups += seen_groups.values()

		# Вычисление дэшборда для администраторов
		t_start = current_milli_time()	
		self.calculate_admin_dashboard()
		self.print_message("Took %s ms to calculate admin dashboard, now calculating brands and shops dashboards..." % (current_milli_time() - t_start,))

		# Вычисление дэшбордов для брендов и магазинов
		for brand_tuple in brands:
			t_start = current_milli_time()	
			self.calculate_brand_dashboard(brand_tuple[0])
			self.print_message("Took %s ms to calculate dashboard for brand %s, now calculating shops dashboards..." % (current_milli_time() - t_start, brand_tuple[0]))
			
			self.cur.execute(""" SELECT id FROM shop WHERE company_id = %s """, (brand_tuple[0], ))
			shops = self.cur.fetchall()
			t_start = current_milli_time()
			for shop_tuple in shops:
				t_start = current_milli_time()	
				self.calculate_shop_dashboard(shop_tuple[0], brand_tuple[0])
			print "Took %s ms to calculate shops dashboards for brand %s" % (current_milli_time() - t_start, brand_tuple[0])

		if CLEAR_TEMPORARY_COLLECTIONS is True:
			for c in [DASHBOARD_CHEQUES_MONTH_COLLECTION, AGGREGATED_CARDS_BY_PURCHASES_SHOPS, AGGREGATED_CARDS_BY_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_SHOPS, AGGREGATED_RE_PURCHASES_BRANDS, AGGREGATED_RE_PURCHASES_TOTALS_SHOPS, AGGREGATED_RE_PURCHASES_TOTALS_BRANDS, AGGREGATED_CHEQUES_PER_PERIOD]:
				self.client[self.db_name][c].drop()
			for w in range(8):
				self.client[self.db_name][WEEKS_SALES_COLLECTION_PREFIX + str(w)].drop()

		self.print_message("The time is %s, took %s ms to complete" % (time.ctime(), current_milli_time() - run_start))


	def get_shops_postgre_data(self):
		t_start = current_milli_time()
		""" Соотношение полов """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN p.gender = \'m\' THEN 1 ELSE 0 END) as male,
			SUM(CASE WHEN p.gender = \'f\' THEN 1 ELSE 0 END) as female,
			SUM(CASE WHEN p.gender = \'u\' THEN 1 ELSE 0 END) as undefined,
			COUNT(p.digital_id) as total,
			c.shop_id as shop_id,
			c.company_id
			FROM personal_data p 
			JOIN customer cst ON cst.digital_id = p.digital_id 
			JOIN card c ON c.customer_id = cst.id
			GROUP BY c.shop_id, c.company_id""")
		self.genders_data = self.cur.fetchall()
		self.print_message("Took %s ms to get genders data from POSTGRESQL DB" % (current_milli_time() - t_start, ))


	def get_brands_postgre_data(self):
		t_start = current_milli_time()
		""" Количество добавленнных карт """
		""" Доли каналов коммуникации и согласных на рассылку """
		""" Доля заполненных анкет """
		""" Количество участников """
		self.cur.execute(""" SELECT 
			SUM(CASE WHEN t.channel = 3 THEN 1 ELSE 0 END)::FLOAT AS all, 
			SUM(CASE WHEN t.channel = 1 THEN 1 ELSE 0 END)::FLOAT AS email, 
			SUM(CASE WHEN t.channel = 2 THEN 1 ELSE 0 END)::FLOAT AS sms,
			SUM(CASE WHEN t.channel = 0 THEN 1 ELSE 0 END)::FLOAT AS unknown,
			SUM(CASE WHEN t.subscription THEN 1 ELSE 0 END)::FLOAT as subscribed,
			SUM(CASE WHEN t.date_create >= %s THEN 1 ELSE 0 END) as month,
			SUM(CASE WHEN t.date_create >= %s THEN 1 ELSE 0 END) as week,
			SUM(CASE WHEN t.date_create >= %s THEN 1 ELSE 0 END) as day,
			SUM(CASE WHEN c.fullness = 0 THEN 1 ELSE 0 END)::FLOAT AS not_filled, 
			SUM(CASE WHEN c.fullness = 1 THEN 1 ELSE 0 END)::FLOAT AS partially_filled, 
			SUM(CASE WHEN c.fullness = 2 THEN 1 ELSE 0 END)::FLOAT AS filled, 
			COUNT(c.fullness) as profiles,
			COUNT(t.id) as total,
			t.company_id,
			t.shop_id
			FROM card t JOIN customer c 
			ON t.customer_id = c.id 
			GROUP BY t.company_id, t.shop_id """, (self.month_start_ts, self.week_start_ts, self.yesterday_ts))
		self.cards_data = self.cur.fetchall()
		self.print_message("Took %s ms to get cards data from POSTGRESQL DB" % (current_milli_time() - t_start, ))


	def run_aggregators(self):
		""" Продажи за 8 недель """
		sales_updater = admin_updaters.SalesUpdater()
		sales_updater.run()

		""" Число чеков за месяц, неделю и вчера """
		cheques_per_period_updater = shops_updaters.ChequesPerPeriod()
		cheques_per_period_updater.run()

		""" Доли клиентов с определенным числом покупок (для магазинов) """
		customers_by_purchases_updater = shops_updaters.CustomersByPurchasesUpdater(split_keys=self.splitted_keys)
		customers_by_purchases_updater.run()

		""" Доли клиентов с определенным числом покупок (для брендов) """
		customers_by_purchases_updater = brands_updaters.CustomersByPurchasesUpdater(split_keys=self.splitted_keys)
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
		total 		= sum([data['total'] for data in self.cards_data])
		percents	= {
			'channels': {
				'all': 			0 if total == 0 else 100.0 * sum([data['all'] for data in self.cards_data]) / total,
				'email': 		0 if total == 0 else 100.0 * sum([data['email'] for data in self.cards_data]) / total,
				'sms': 			0 if total == 0 else 100.0 * sum([data['sms'] for data in self.cards_data]) / total,
				'unknown': 	0 if total == 0 else 100.0 * sum([data['unknown'] for data in self.cards_data]) / total,
			},
			'subscribed': 0 if total == 0 else 100.0 * sum([data['subscribed'] for data in self.cards_data]) / total,
		}

		dashboard_data['channels_data'] = percents['channels']
		dashboard_data['subscribed'] 		= percents['subscribed']


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
		total 		= sum([data['total'] for data in self.genders_data])
		percents	= {
			'male': 			0 if total == 0 else 100.0 * sum([data['male'] for data in self.genders_data]) / total,
			'female': 		0 if total == 0 else 100.0 * sum([data['female'] for data in self.genders_data]) / total,
			'undefined': 	0 if total == 0 else 100.0 * sum([data['undefined'] for data in self.genders_data]) / total,
		}

		dashboard_data['genders_data'] = percents


		""" Клиентская база """
		total = sum([data['total'] for data in self.cards_data if data['company_id'] in self.brands_groups])
		dashboard_data['participants_count'] = total


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
		percents 		= {}
		self.cur.execute(""" SELECT id as company_id, name FROM company """)
		brands_names = self.cur.fetchall()
		total = sum([pair['total'] for pair in self.cards_data])
		for brand in brands_names:			
			percents[brand['name']] = 0 if total == 0 else 100.0 * sum([d['total'] for d in self.cards_data if d['company_id'] == brand['company_id']]) / total
		dashboard_data['participants_by_brands'] = percents


		self.update_dashboard({"brand_id": 0}, dashboard_data)


	def calculate_brand_dashboard(self, brand_id):
		dashboard_data = {
			'brand_id': brand_id,
			'type'		: 'operator'
		}

		brand_cards_data 		= [d for d in self.cards_data if d['company_id'] == brand_id]
		brand_genders_data 	= [d for d in self.genders_data if d['company_id'] == brand_id]

		""" Соотношение каналов коммуникации и доля согласных на рассылку """
		total = sum([d['total'] for d in brand_cards_data])
		percents = {
			'channels': {
				'all': 			0 if total == 0 else 100.0 * sum([d['all'] for d in brand_cards_data]) / total,
				'email': 		0 if total == 0 else 100.0 * sum([d['email'] for d in brand_cards_data]) / total,
				'sms': 			0 if total == 0 else 100.0 * sum([d['sms'] for d in brand_cards_data]) / total,
				'unknown': 	0 if total == 0 else 100.0 * sum([d['unknown'] for d in brand_cards_data]) / total,
			},
			'subscribed': 0 if total == 0 else 100.0 * sum([d['subscribed'] for d in brand_cards_data]) / total,
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
			total = sum([d['total'] for d in brand_cards_data])
			for key in ['qty_1', 'qty_2', 'qty_3']:
				cards_with_cheques += aggregated[key]
				percents[key]['value'] = 0 if total == 0 else 100.0 * aggregated[key] / total
			no_cheques = total - cards_with_cheques
			percents['qty_0']['value'] = 0 if total == 0 else 100.0 * no_cheques / total
		dashboard_data['progress_data'] = percents


		""" Доля повторных покупок с начала месяца """
		not_firsts 	= self.client[self.db_name][AGGREGATED_RE_PURCHASES_BRANDS].find_one({'brand_id': brand_id})	
		totals_data = self.client[self.db_name][AGGREGATED_RE_PURCHASES_TOTALS_BRANDS].find_one({'brand_id': brand_id})	
		total_sum 	= 0 if totals_data is None else totals_data['has_card'] + totals_data['no_card']
		percent 		= 0 if total_sum == 0 else 100.0 * (totals_data['has_card'] - not_firsts['not_firsts']) / total_sum
		dashboard_data['re_purchases_month'] = percent


		""" Доля заполненных анкет """
		total = sum([d['profiles'] for d in brand_cards_data])
		dashboard_data['filled_profiles'] = 0 if total == 0 else 100.0 * sum([d['filled'] for d in brand_cards_data]) / total


		""" Доля продаж по картам с начала месяца """
		purchases_with_cards_updater = brands_updaters.PurchasesWithCardsUpdater(brand_id, self.month_start_ts, self.today_ts)
		purchases_with_cards_updater.run()

		aggregated 	= purchases_with_cards_updater.find_aggregated_data()	
		percent 		= 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'] / aggregated['denominator']
		dashboard_data['purchased_with_cards'] = percent


		""" Число участников """
		dashboard_data['participants_count'] = sum([d['total'] for d in brand_cards_data])

		""" Доля выданных бонусных карт с начала месяца """
		data = {
			'month': 	sum([d['month'] for d in brand_cards_data]),
			'week':		sum([d['week'] for d in brand_cards_data]),
			'day': 		sum([d['day'] for d in brand_cards_data]),
		}
		result = self.client[self.db_name][AGGREGATED_CHEQUES_PER_PERIOD].aggregate([
			{
				'$match': {'brand_id': brand_id}
			},
			{
				'$group': {
					'_id': 		None,
					'month':	{"$sum": '$month'},
					'week':		{"$sum": '$week'},
					'day':		{"$sum": '$day'}
				}
			}
		])
		cheques = {'month': 0, 'week': 0, 'day': 0} if not result['result'] else result['result'][0]
		dashboard_data['cards_added_month'] = 0 if cheques['month'] == 0 else 100.0 * data['month'] / cheques['month']
		dashboard_data['cards_added_week'] 	= 0 if cheques['week'] == 0 else 100.0 * data['week'] / cheques['week']
		dashboard_data['cards_added_day'] 	= 0 if cheques['day'] == 0 else 100.0 * data['day'] / cheques['day']


		""" Соотношение полов """
		total = sum([d['total'] for d in brand_genders_data])
		percents = {
			'male': 			0 if total == 0 else 100.0 * sum([d['male'] for d in brand_genders_data]) / total,
			'female': 		0 if total == 0 else 100.0 * sum([d['female'] for d in brand_genders_data]) / total,
			'undefined': 	0 if total == 0 else 100.0 * sum([d['undefined'] for d in brand_genders_data]) / total,
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


		""" Соотношение заполненных анкет """
		total = sum([d['total'] for d in brand_cards_data])
		percents = {
			'not_filled': 			0 if total == 0 else 100.0 * sum([d['not_filled'] for d in brand_cards_data]) / total,
			'partially_filled': 0 if total == 0 else 100.0 * sum([d['partially_filled'] for d in brand_cards_data]) / total,
			'filled': 					0 if total == 0 else 100.0 * sum([d['filled'] for d in brand_cards_data]) / total,
		}

		dashboard_data['filled_data'] = percents


		self.update_dashboard({"brand_id": brand_id}, dashboard_data)


	def calculate_shop_dashboard(self, shop_id, brand_id):
		dashboard_data = {
			'brand_id': brand_id,
			'shop_id'	: shop_id,
			'type'		: 'seller'
		}

		shop_cards_data 	= [d for d in self.cards_data if d['shop_id'] == shop_id]
		shop_genders_data = [d for d in self.genders_data if d['shop_id'] == shop_id]

		""" Соотношение каналов коммуникации и доля согласных на рассылку """
		total = sum([d['total'] for d in shop_cards_data])
		percents = {
			'channels': {
				'all': 			0 if total == 0 else 100.0 * sum([d['all'] for d in shop_cards_data]) / total,
				'email': 		0 if total == 0 else 100.0 * sum([d['email'] for d in shop_cards_data]) / total,
				'sms': 			0 if total == 0 else 100.0 * sum([d['sms'] for d in shop_cards_data]) / total,
				'unknown': 	0 if total == 0 else 100.0 * sum([d['unknown'] for d in shop_cards_data]) / total,
			},
			'subscribed': 0 if total == 0 else 100.0 * sum([d['subscribed'] for d in shop_cards_data]) / total,
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
			total = sum([d['total'] for d in shop_cards_data])
			for key in ['qty_1', 'qty_2', 'qty_3']:
				cards_with_cheques += aggregated[key]
				percents[key]['value'] = 0 if total == 0 else 100.0 * aggregated[key] / total
			no_cheques = total - cards_with_cheques			
			percents['qty_0']['value'] = 0 if total == 0 else 100.0 * no_cheques / total

		dashboard_data['progress_data'] = percents


		""" Доля повторных покупок с начала месяца """
		not_firsts 	= self.client[self.db_name][AGGREGATED_RE_PURCHASES_SHOPS].find_one({'shop_id': shop_id})	or {'not_firsts': 0}
		totals_data = self.client[self.db_name][AGGREGATED_RE_PURCHASES_TOTALS_SHOPS].find_one({'shop_id': shop_id}) or {'has_card': 0, 'no_card': 0}
		total_sum 	= 0 if totals_data is None else totals_data['has_card'] + totals_data['no_card']
		percent 		= 0 if total_sum == 0 else 100.0 * (totals_data['has_card'] - not_firsts['not_firsts']) / total_sum
		dashboard_data['re_purchases_month'] = percent


		""" Доля заполненных анкет """
		total = sum([d['profiles'] for d in shop_cards_data])
		dashboard_data['filled_profiles'] = 0 if total == 0 else 100.0 * sum([d['filled'] for d in shop_cards_data]) / total


		""" Доля продаж по картам с начала месяца """
		purchases_with_cards_updater = shops_updaters.PurchasesWithCardsUpdater(brand_id, self.month_start_ts, self.today_ts)
		purchases_with_cards_updater.run()

		aggregated 	= purchases_with_cards_updater.find_aggregated_data()	
		percent 		= 0 if aggregated['denominator'] == 0 else 100.0 * aggregated['numerator'] / aggregated['denominator']
		dashboard_data['purchased_with_cards'] = percent


		""" Число участников """
		dashboard_data['participants_count'] = sum([d['total'] for d in shop_cards_data])


		""" Доли выданных бонусных карт с начала месяца, недели и вчерашнего дня """
		data = {
			'month': 	sum([d['month'] for d in shop_cards_data]),
			'week':		sum([d['week'] for d in shop_cards_data]),
			'day': 		sum([d['day'] for d in shop_cards_data]),
		}
		cheques = self.client[self.db_name][AGGREGATED_CHEQUES_PER_PERIOD].find_one({'shop_id': shop_id}) or {'month': 0, 'week': 0, 'day': 0}
		dashboard_data['cards_added_month'] = 0 if cheques['month'] == 0 else 100.0 * data['month'] / cheques['month']
		dashboard_data['cards_added_week'] 	= 0 if cheques['week'] == 0 else 100.0 * data['week'] / cheques['week']
		dashboard_data['cards_added_day'] 	= 0 if cheques['day'] == 0 else 100.0 * data['day'] / cheques['day']


		""" Соотношение полов """
		total = sum([d['total'] for d in shop_genders_data])
		percents = {
			'male': 			0 if total == 0 else 100.0 * sum([d['male'] for d in shop_genders_data]) / total,
			'female': 		0 if total == 0 else 100.0 * sum([d['female'] for d in shop_genders_data]) / total,
			'undefined': 	0 if total == 0 else 100.0 * sum([d['undefined'] for d in shop_genders_data]) / total,
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