import argparse
import platform
import logging
import unittest
from operator import add
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, concat_ws, stddev_pop, avg, rank, col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from datetime import datetime, timedelta
import urllib.parse

HOUR = timedelta(hours=3600)
TOP_N = 25

# source data
src_url = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"

# pageview columns and schema
domain_code = "domain_code"
page_title = "page_title"
count_views = "count_views"
total_response_size = "total_response_size"

schema = StructType([
	StructField(domain_code, StringType()),
	StructField(page_title, StringType()),
	StructField(count_views, IntegerType()),
	StructField(total_response_size, IntegerType())
	])

# blacklist columns and schema
encoded_page_title = "encoded_page_title"
blacklist_schema = StructType([
	StructField(domain_code, StringType()),
	StructField(encoded_page_title, StringType())
	])

# udfs
decode_udf = udf(lambda pt: urllib.parse.unquote(pt))
encode_udf = udf(lambda pt: urllib.parse.quote(pt))



# raw columns
pudt = 'tpep_pickup_datetime'
dodt = 'tpep_dropoff_datetime'
ratecodeid = 'ratecodeid'
puid = 'pulocationid'
doid = 'dolocationid'
payment_type = 'payment_type'
total_amount = 'total_amount'
month_year = 'month_year'

# transformed columns
duration = 'duration'
key_pair = 'key_pair'


def calc_duration(dtstr1, dtstr2):
	"""
	dts are of the form YYYY-mm-DD HH:MM:SS
	e.g. 2019-02-01 00:59:04
	:param dt1:
	:param dt2:
	:return:
	"""
	fmt = '%Y-%m-%d %H:%M:%S'
	dt1 = datetime.strptime(dtstr1, fmt)
	dt2 = datetime.strptime(dtstr2, fmt)
	return (dt2-dt1).seconds


def add_month_year(dtstr):
	fmt = '%Y-%m-%d %H:%M:%S'
	dt = datetime.strptime(dtstr, fmt)
	return "{}_{}".format(dt.month, dt.year)


def transform_and_aggregate(df):
	"""
	data is already "filtered" down to month and yellowcab only
	take columns:
		tpep_pickup_datetime
		tpep_dropoff_datetime
		ratecodeid
		pulocationid
		dolocationid
		payment_type
		total_amount
	additional filters to apply:
		for simplification purposes, we will only consider the following categories
		ratecodeid - only take 1=standard rate
		payment_type - only take 1=credit card and 2=cash
	some data considerations:
		according to https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page
		there is a $1 "rush hour" surcharge from 4pm-8pm on weekdays, excluding holidays
		there is a 50 cents overnight surcharge from 8pm to 6am
		there is no extra charge for extra customers
	add column - duration in seconds (tpep_dropoff_datetime - tpep_pickup_datetime)
	group df by
		pulocationid
		dolocationid
		truncated hour
	aggregate by
		avg duration
		avg total_amount
	return dataframe of tuples (pulocationid, dolocationid, avg duration, avg total amount)

	:param df: dataframe of yellowcab taxi data
	:return: df
	"""

	df = df.select([pudt, dodt, ratecodeid, puid, doid, payment_type, total_amount])
	df = df.filter(f'{ratecodeid} = 1')
	df = df.filter(f'{payment_type} = 1 or {payment_type} = 2')
	calc_duration_udf = udf(calc_duration, returnType=IntegerType())
	add_month_year_udf = udf(add_month_year)
	df = df.withColumn(duration, calc_duration_udf(pudt, dodt))
	df = df.withColumn(month_year, add_month_year_udf(pudt))
	aggdf = df.groupBy(puid, doid, month_year).agg(avg(duration), avg(total_amount), stddev_pop(duration), stddev_pop(total_amount))
	aggdf = aggdf.withColumn(key_pair, concat_ws('-', puid, doid, month_year))

	return aggdf

def clean_pageview(pv):
	return pv.withColumn(encoded_page_title, encode_udf(page_title))

def clean_blacklist(bl):
	return bl.withColumn(page_title, decode_udf(encoded_page_title))

def remove_blacklist(df, bl):
	# joined = df.join(bl, [domain_code, page_title], 'left_outer')
	# joined.show(n=20)
	joined = df.join(bl, [domain_code, encoded_page_title], 'left_anti')
	return joined

def window_and_sort(df, n):
	window = Window.partitionBy(df[domain_code]).orderBy(df[count_views].desc())
	ranked = df.withColumn("rank", rank().over(window)).filter(col('rank') <= n)
	return ranked


def run_batch_old(s3_input_file, elasticache_output_location):
	"""
	yellowcab taxi data is approximately 630 mb per file
	fhv high volume data is approximately 1.2 gb per file
	we will only concern ourselves with yellowcab data for now

	read yellowcab taxi data file from s3 into dataframe
		data will be at e.g. s3://stam-dshaw-raw/yellow_tripdata_2019-02.csv
	apply transformation
	load to elasticache

	:param s3_input_file:
	:return:
	"""
	spark = SparkSession.builder.appName("TaxiAggregator").getOrCreate()
	print(f"reading {s3_input_file}")
	df = spark.read.csv(s3_input_file, header=True)
	print("aggregating")
	aggdf = transform_and_aggregate(df)
	print("showing")
	aggdf.show(n=10)
	print("writing to redis")
	aggdf.write.format("org.apache.spark.sql.redis").option("table", "taxiagg").option("key.column", key_pair).save(mode='overwrite')
	print("stopping spark")
	spark.stop()
	return


def validate_input(start, end):
	fmt = '%Y%m%d%H'
	sdt = datetime.strptime(start, fmt)
	edt = datetime.strptime(end, fmt)
	if sdt > edt:
		raise Exception("start must be before end")
	return sdt, edt

def check_cache(dt):
	return False

def download_and_gunzip(dt):
	pass

def sink_data(tranformed):
	pass

def run_batch(start, end):
	"""
	for each date-hour
		if date-hour not in outputs folder
			download source datea
			gunzip
			transform
			write to outputs folder
	"""
	sdt, edt = validate_input(start, end)
	bl = self.spark.read.csv("blacklist", schema=blacklist_schema, sep=" ")
	while sdt <= edt:
		if not check_cache(sdt):
			source_data = download_and_gunzip(sdt)
			pv = self.spark.read.csv(source_data, schema=schema, sep = " ")
			clean_pv = clean_pageview(pv)

			bl_removed = remove_blacklist(clean_pv, bl)

			transformed = window_and_sort(bl_removed, TOP_N)
			sink_data(transformed)
		else:
			print(f"{sdt} already processed, skipping...")
		sdt += HOUR
	print("all done.")

def main():
	print("############################")
	print("Welcome to DD2020 Wikipedia Aggregator")
	print("You are using python version {}".format(platform.python_version()))
	print("############################")
	parser = argparse.ArgumentParser(description="Taxi Cost and Time Aggregator")
	parser.add_argument('--start', dest="start", help="start date and hour in YYYYMMDDHH format", type=str, required=True)
	parser.add_argument('--end', dest="end", help="end date and hour in YYYYMMDDHH format", type=str, required=True)
	args = parser.parse_args()
	run_batch(args.start, args.end)


class PySparkTest(unittest.TestCase):
	@classmethod
	def supress_py4j_logging(cls):
		logger = logging.getLogger('py4j')
		logger.setLevel(logging.WARN)

	@classmethod
	def create_testing_pyspark_session(cls):
		return SparkSession.builder.master('local').appName('unittest-pyspark').enableHiveSupport().getOrCreate()

	@classmethod
	def setUpClass(cls):
		cls.supress_py4j_logging()
		cls.spark = cls.create_testing_pyspark_session()

	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()


class WikiAggregatorTest(PySparkTest):
	def test_sanity(self):
		test_rdd = self.spark.sparkContext.parallelize(['cat dog mouse','cat cat dog'], 2)
		results = test_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(add).collect()
		expected_results = [('cat', 3), ('dog', 2), ('mouse', 1)]
		self.assertEqual(set(results), set(expected_results))

	def test_aggregate_domain(self):
		pv = self.spark.read.csv("pageview-sample", schema=schema, sep = " ")
		pv.printSchema()
		print("data types: {}".format(pv.dtypes))
		pv.show(n=20)
		pv_count = pv.count()
		print(f"count pageview {pv_count}")

		clean_pv = clean_pageview(pv)
		clean_pv.show(n=20)
		clean_pv_count = clean_pv.count()
		print(f"count clean pageview {clean_pv_count}")


		bl = self.spark.read.csv("blacklist", schema=blacklist_schema, sep=" ")
		bl.show(n=20)
		print("count blacklist {}".format(bl.count()))
		#clean_bl = clean_blacklist(bl)
		#clean_bl.show(n=20)

		bl_removed = remove_blacklist(clean_pv, bl)
		bl_removed.show(n=20)
		bl_removed_count = bl_removed.count()
		print(f"count bl removed {bl_removed_count}")

		transformed = window_and_sort(bl_removed, 3)
		transformed.show(n=21)
		transformed_count = transformed.count()
		print(f"count transformed {transformed_count}")

	"""
	def test_aggregate_cost(self):
		fhv = self.spark.read.csv("yellow_tripdata_sample.csv", header=True)
		fhv.printSchema()
		print("data types: {}".format(fhv.dtypes))
		fhv.show(n=20)
		print("count {}".format(fhv.count()))

		processed = transform_and_aggregate(fhv)
		print("data types: {}".format(processed.dtypes))
		processed.show(n=20)
		print("count {}".format(processed.count()))

		# there are 4 trips going from 264 to 264 in the sample dataset

		# amounts are 12.96, 10.35, 16.8
		avg_amount = (12.96 + 10.35 + 16.8 + 7.8)/4

		# times are
		# 2019-02-01 0:15:55	2019-02-01 0:26:26
		# 2019-02-01 0:31:38	2019-02-01 0:37:59
		# 2019-02-01 0:42:13	2019-02-01 0:59:37
		# 2019-02-01 0:45:10	2019-02-01 0:51:12
		avg_duration = ((10*60)+5+26 + (6*60)+21 + (17*60)+24 + (6*60)+2)/4
		filtered = processed.filter('key_pair="264-264-2_2019"').collect()
		self.assertEqual(len(filtered), 1)
		self.assertEqual(filtered[0]['avg(duration)'], avg_duration)
		self.assertEqual(filtered[0]['avg(total_amount)'], avg_amount)
	"""

if __name__ == '__main__':
	main()