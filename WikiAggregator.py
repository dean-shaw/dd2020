import argparse
import platform
import logging
import unittest
from operator import add
from pyspark import version as pyv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, rank, col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from datetime import datetime, timedelta
import urllib.parse
from gzip import decompress
from requests import get
from os import path

HOUR = timedelta(hours=1)
TOP_N = 25

# source data
src_url = "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"

# filenames
in_raw = "pageviews-{year}{month}{day}-{hour}.csv"
out_processed = "top{n}-{year}{month}{day}-{hour}.csv"

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
encode_udf = udf(lambda pt: urllib.parse.quote(pt))


def lpad(i):
    """
    left pad integers (specifically day/month/hour) with 0s

    :param i: integer to pad
    :return: 2 digit i, left padded with zeroes
    """
    s = "0" + str(i)
    return s[-2:]


def clean_pageview(pv):
    """
    url decode the raw page title from pageviews

    :param pv: dataframe of raw pageview data
    :return: pageview dataframe with an additional url encoded pagetitle column
    """
    print("cleaning pageview")
    return pv.withColumn(encoded_page_title, encode_udf(page_title))


def remove_blacklist(df, bl):
    """
    remove rows from pageview whos page title and domain code are matched in blacklist

    :param df: cleaned pageview dataframe
    :param bl: blacklist dataframe
    :return: pageview dataframe with blacklist removed
    """
    print("removing blacklist")
    joined = df.join(bl, [domain_code, encoded_page_title], 'left_anti')
    return joined


def window_and_sort(df, n):
    """
    include only top N page titles per domain code.
    window by domain code and add rank, then filter.
    note: if several results are tied, they will all be returned

    :param df: pageview dataframe to window and sort
    :param n: top N to return per domain code
    :return: pageview dataframe with only top N per domain code
    """
    print("windowing and sorting")
    window = Window.partitionBy(df[domain_code]).orderBy(df[count_views].desc())
    ranked = df.withColumn("rank", rank().over(window)).filter(col('rank') <= n)
    return ranked


def validate_input(start, end):
    """
    helper function to ensure valid start and end times are inputted

    :param start: start timestamp
    :param end: end timestamp
    :return: True if dates are valid
    """
    fmt = '%Y%m%d%H'
    sdt = datetime.strptime(start, fmt)
    edt = datetime.strptime(end, fmt)
    if sdt > edt:
        raise Exception("start must be before end")
    return sdt, edt


def check_cache(year, month, day, hour):
    """
    check if this result set has already been computed.
    Results are saved to local disk, so we just check local disk

    :param year:
    :param month:
    :param day:
    :param hour:
    :return: True if file exists locally
    """
    return path.exists(out_processed.format(n=TOP_N, year=year, month=month, day=day, hour=hour))


def download_and_gunzip(year, month, day, hour):
    """
    download the raw pageview data corresponding to this datetime,decompress, and save to disk

    :param year: 
    :param month: 
    :param day: 
    :param hour: 
    :return: local filepath to downloaded data
    """
    full_url = src_url.format(year=year, month=month, day=day, hour=hour)
    print(f"downloading from {full_url}")
    download_location = in_raw.format(year=year, month=month, day=day, hour=hour)
    with open(download_location, 'wb') as f:
        f.write(decompress(get(full_url).content))
    print(f"done writing to {download_location}")
    return download_location


def run_batch(start, end):
    """
    for each date-hour
        if date-hour has not been computed already
            download and gunzip source data to disk
            transform
            write results to disk
    :param start: start date and time in YYYYmmddHH format
    :param end: end date and time in YYYYmmddHH format
    :return:
    """
    sdt, edt = validate_input(start, end)
    spark = SparkSession.builder.appName("WikiAggregator").getOrCreate()
    bl = spark.read.csv("blacklist", schema=blacklist_schema, sep=" ")

    while sdt <= edt:
        month = lpad(sdt.month)
        day = lpad(sdt.day)
        hour = lpad(sdt.hour)
        if not check_cache(sdt.year, month, day, hour):
            source_data = download_and_gunzip(sdt.year, month, day, hour)
            pv = spark.read.csv(source_data, schema=schema, sep = " ")

            clean_pv = clean_pageview(pv)
            bl_removed = remove_blacklist(clean_pv, bl)
            transformed = window_and_sort(bl_removed, TOP_N)
            sort_by_domain_and_rank = transformed.orderBy(domain_code, "rank")

            write_location = out_processed.format(n=TOP_N, year=sdt.year, month=month, day=day, hour=hour)
            print(f"sinking to {write_location}")
            sort_by_domain_and_rank.coalesce(1).write.csv(write_location, header=True)
        else:
            print(f"{sdt} already processed, skipping...")
        sdt += HOUR

    print("stopping spark")
    spark.stop()
    print("all done.")
    return


def main():
    print("######################################################")
    print("Welcome to DD2020 Wikipedia Aggregator")
    print("You are using pyspark version {}".format(pyv.__version__))
    print("On python version {}".format(platform.python_version()))
    print("######################################################")
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

        bl_removed = remove_blacklist(clean_pv, bl)
        bl_removed.show(n=20)
        bl_removed_count = bl_removed.count()
        print(f"count bl removed {bl_removed_count}")

        transformed = window_and_sort(bl_removed, 3)
        transformed.show(n=21)
        transformed_count = transformed.count()
        print(f"count transformed {transformed_count}")


if __name__ == '__main__':
    main()