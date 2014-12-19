#!/usr/bin/env python
from multiprocessing import Process
from pymongo import MongoClient
import time, math, json, copy
from config import MONGO_CLIENT_HOST, MONGO_DB_NAME, DASHBOARD_CHEQUES_COLLECTION

current_milli_time = lambda: int(round(time.time() * 1000))


class DashboardProcessor(object):
	pipeline = []

	def __init__(self, map_key_name, start_ts=None, end_ts=None):
		self.threads = []
		self.collections = []
		self.result = {}
		self.map_key_name = map_key_name
		self.start_ts = start_ts
		self.end_ts = end_ts
		self.collections = []
		self.client = MongoClient(MONGO_CLIENT_HOST)
		self.db = self.client[MONGO_DB_NAME]
		self.subset_collection = 'dashboard_processor_subset'
		self.target_collection = 'dashboard_python_results'
		self.source_collection = DASHBOARD_CHEQUES_COLLECTION
		self.NUM_THREADS = 4
		self.DEBUG = False

	def run(self, split_keys=None):
		if self.DEBUG:
			print "===== Dashboard processor started at %s" % time.ctime()
		t_start = current_milli_time()
		start = t_start
		if split_keys is None:
			self.split_keys()
			if self.DEBUG:
				print "===== Splitting keys has taken %s ms to complete" % (current_milli_time() - t_start)
		else:
			self.split_keys = split_keys
			if self.DEBUG:
				print "No need to split keys"
		t_start = current_milli_time()
		self.run_threads()
		if self.DEBUG:
			print "===== Running threads has taken %s ms to complete" % (current_milli_time() - t_start)
		t_start = current_milli_time()
		results = self.process_results()
		if self.DEBUG:
			print "===== Processing results has taken %s ms to complete" % (current_milli_time() - t_start)
		t_start = current_milli_time()
		self.save_data(results)
		if self.DEBUG:
			print "===== Saving data has taken %s ms to complete" % (current_milli_time() - t_start)
		t_start = current_milli_time()
		self.drop_subset_collections()
		if self.DEBUG:
			print "===== Dropping subset collections has taken %s ms to complete" % (current_milli_time() - t_start)
			print "===== Processing was completed in %s ms" % (current_milli_time() - start)

	def split_keys(self):
		self.split_keys = self.db.command(
			"splitVector", MONGO_DB_NAME + "." + self.source_collection,
			keyPattern={self.map_key_name: 1},
			maxChunkSizeBytes=32000
		)['splitKeys']

	def run_threads(self):
		inc = int(math.floor(len(self.split_keys) / self.NUM_THREADS) + 1)
		for i in range(self.NUM_THREADS):
			if (i == 0):
				min_v = ""
			else:
				min_v = self.split_keys[i * inc][self.map_key_name]

			if (i * inc + inc >= len(self.split_keys)):
				max_v = -1
			else:
				max_v = self.split_keys[i * inc + inc][self.map_key_name]

			# print "min: %s, max: %s" % (min_v, max_v)
			self.collections.append(self.subset_collection + str(min_v))
			t = Process(target=self.aggregate_subset, args=(min_v, max_v))
			self.threads.append(t)
			t.start()

		for t in self.threads:
			t.join()
		# print t

	def process_results(self):
		return []

	def save_data(self, data):
		self.result = data
		self.db[self.target_collection].update(
			{"name": self.name},
			{
				"name": self.name,
				"data": data
			}, upsert=True
		)

	def drop_subset_collections(self):
		for collection_name in self.collections:
			self.db[collection_name].drop()


	def aggregate_subset(self, min_v, max_v, subset_suffix=None):
		subset_collection_name = self.subset_collection + (str(min_v) if subset_suffix is None else str(subset_suffix))

		single_key_match = {}
		single_key_match[self.map_key_name] = {"$gte": min_v} if (max_v < 0) else {"$gte": min_v, "$lt": max_v};

		single_key_sort = {}
		single_key_sort[self.map_key_name] = 1;

		new_pipeline = copy.deepcopy(self.pipeline)

		new_pipeline.insert(0, {"$sort": single_key_sort})
		new_pipeline.insert(0, {"$match": single_key_match})
		if self.start_ts is not None and self.end_ts is not None:
			new_pipeline.insert(0, {"$match": {"date": {"$gte": self.start_ts, "$lt": self.end_ts}}})
		elif self.start_ts is not None:
			new_pipeline.insert(0, {"$match": {"date": {"$gte": self.start_ts}}})
		new_pipeline.append({"$out": subset_collection_name})

		result = self.db.command("aggregate", self.source_collection, pipeline=new_pipeline, allowDiskUse=True)
		# print json.dumps(result['result'][0], sort_keys=True, indent=4, separators=(',', ': '))
		# print json.dumps(new_pipeline, sort_keys=True, indent=4, separators=(',', ': '))
		return {'outCollection': subset_collection_name, 'subsetResult': result}