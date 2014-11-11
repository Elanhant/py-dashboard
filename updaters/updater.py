#!/usr/bin/env python
from processors.processor import DashboardProcessor
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import psycopg2
from config import MONGO_CLIENT_HOST, MONGO_DB_NAME, POSTGRESQL_CONN_STRING, UPDATER_COLLECTION

class Updater(object):
	def __init__(self):		
		client 				= MongoClient(MONGO_CLIENT_HOST)
		self.db_name 	= MONGO_DB_NAME
		self.db 			= client[self.db_name]
		postgres_conn = psycopg2.connect(POSTGRESQL_CONN_STRING)
		self.cur 			= postgres_conn.cursor()
		self.name 		= 'dashboard_updater'
		self.CHECK_DATE = False
		self.aggregated_data = None
		self.collection_name = UPDATER_COLLECTION

	def run(self):
		today 				= datetime.today().replace(hour=0, minute=0, second=0)
		today_ts 			= int(time.mktime(today.timetuple()))
		yesterday 		= today - timedelta(days=1)	
		yesterday_ts 	= int(time.mktime(yesterday.timetuple()))

		self.aggregated_data = self.find_aggregated_data()
		if self.aggregated_data is None:
			""" Have to calculate data since the very beginning """
			self.calculate_total_data()
		elif self.CHECK_DATE is False or self.aggregated_data['updated'] < today_ts:
			""" A new day has come, time to recalculate """
			self.calculate_day_data(today_ts, yesterday_ts)	 
		else:
			""" No need to update """
			pass


	def update_aggregated_data(self, numerator, denominator, last_ts = None):
		self.db[self.collection_name].update(
			{"name": self.name},
			{
				'name': 				self.name,
				'numerator': 		numerator,
				'denominator': 	denominator,
				'updated': 			int(time.time()),
				'last_ts':			last_ts
			}, upsert=True
		)
		self.aggregated_data = self.find_aggregated_data()

	def find_aggregated_data(self):
		return self.db[self.collection_name].find_one({'name': self.name})

	def calculate_total_data(self):
		return {}

	def calculate_day_data(self, today_ts, yesterday_ts):
		return {}
