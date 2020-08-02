#!/usr/bin/python3

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
import subprocess
from pyspark.sql.types import IntegerType

import sys

class Performance():
    def __init__(self):
        self.conf = SparkConf().setAppName('Performance App')
        self.sc=SparkContext(conf=self.conf)
        
    def load_year(self, year):        
        spark = SparkSession.builder.appName("performance-app").config("spark.config.option", "value").getOrCreate()
        self.df = spark.read.option("header", "true").csv(year)
        return self.df
    
    def originated_airport_with_most_depDelay(self, out_dir):
        orig_airports = self.df.filter(self.df.DepDelay != 'NA').filter(self.df.DepDelay > 0).select('Origin', 'DepDelay').orderBy(desc('DepDelay'))
        self.sc.parallelize(orig_airports.collect())
        return orig_airports.first()

    def originated_airport_with_least_depDelay(self, out_dir):
        orig_airports = self.df.filter(self.df.DepDelay != 'NA').filter(self.df.DepDelay <= 0).select('Origin', 'DepDelay').orderBy(asc('DepDelay'))
        self.sc.parallelize(orig_airports.collect())
        return orig_airports.first()

    def originated_airport_with_most_arrivalDelay(self, out_dir):
        orig_airports = self.df.filter(self.df.ArrDelay != 'NA').filter(self.df.ArrDelay > 0).select('Origin', 'ArrDelay').orderBy(desc('ArrDelay'))
        #orig_airports = self.df.select('Origin', 'ArrDelay').select(max(F.struct('ArrDelay', *(x for x in self.df.columns if x != 'ArrDelay'))))
        self.sc.parallelize(orig_airports.collect())
        return orig_airports.first()

    def originated_airport_with_least_arrivalDelay(self, out_dir):
        orig_airports = self.df.filter(self.df.ArrDelay != 'NA').filter(self.df.ArrDelay <= 0).select('Origin', 'ArrDelay').orderBy(asc('ArrDelay'))
        self.sc.parallelize(orig_airports.collect())
        return orig_airports.first()

    def originated_flight_with_most_arrivalDelay(self, out_dir):
        orig_flights = self.df.filter(self.df.ArrDelay != 'NA').filter(self.df.ArrDelay > 0).select('FlightNum', 'ArrDelay').orderBy(desc('ArrDelay'))
        self.sc.parallelize(orig_flights.collect())
        return orig_flights.first()

    def originated_flight_with_least_arrivalDelay(self, out_dir):
        orig_flights = self.df.filter(self.df.ArrDelay != 'NA').filter(self.df.ArrDelay <= 0).select('FlightNum', 'ArrDelay').orderBy(asc('ArrDelay'))
        self.sc.parallelize(orig_flights.collect())
        return orig_flights.first()

    def originated_flight_with_most_depDelay(self, out_dir):
        orig_flights = self.df.filter(self.df.DepDelay != 'NA').filter(self.df.DepDelay > 0).select('FlightNum', 'DepDelay').orderBy(desc('DepDelay'))
        #orig_airports = self.df.select('Origin', 'ArrDelay').select(max(F.struct('ArrDelay', *(x for x in self.df.columns if x != 'ArrDelay'))))
        self.sc.parallelize(orig_flights.collect())
        return orig_flights.first()

    def originated_flight_with_least_depDelay(self, out_dir):
        orig_flights = self.df.filter(self.df.DepDelay != 'NA').filter(self.df.DepDelay <= 0).select('FlightNum', 'DepDelay').orderBy(asc('DepDelay'))
        self.sc.parallelize(orig_flights.collect())
        return orig_flights.first()

    def carrier_with_avg_depDelay(self, out_dir):
        carrier = self.df.filter(self.df.DepDelay != 'NA').select('UniqueCarrier', 'DepDelay').groupBy('UniqueCarrier').agg(avg(col("DepDelay")).alias('avgdepDelay'))
        self.sc.parallelize(carrier.collect())
        return carrier.first()

    def carrier_with_avg_arrDelay(self, out_dir):
        carrier = self.df.filter(self.df.ArrDelay != 'NA').select('UniqueCarrier', 'ArrDelay').groupBy('UniqueCarrier').agg(avg(col("ArrDelay")).alias('avgarrDelay'))
        self.sc.parallelize(carrier.collect())
        return carrier.first()
        
def delete_out_dir(out_dir):
    subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])           
        
def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])

    most_dep_delay_airports = perf.originated_airport_with_most_depDelay(argv[1])
    print('{} that expierences the most departure delay'.format(most_dep_delay_airports['Origin']))
    
    least_dep_delay_airports = perf.originated_airport_with_least_depDelay(argv[1])
    print('{} that expierences the least departure delay'.format(least_dep_delay_airports['Origin']))

    most_arrival_delay_airports = perf.originated_airport_with_most_arrivalDelay(argv[1])
    print('{} that expierences the most arrival delay'.format(most_arrival_delay_airports['Origin']))
    
    least_arrival_delay_airports = perf.originated_airport_with_least_arrivalDelay(argv[1])
    print('{} that expierences the least arrival delay'.format(least_arrival_delay_airports['Origin']))

    most_arrival_delay_flight = perf.originated_flight_with_most_arrivalDelay(argv[1])
    print('{} flight that expierences the most arrival delay'.format(most_arrival_delay_flight['FlightNum']))
    
    least_arrival_delay_flight = perf.originated_flight_with_least_arrivalDelay(argv[1])
    print('{} flight that expierences the least arrival delay'.format(least_arrival_delay_flight['FlightNum']))

    most_dep_delay_flight = perf.originated_flight_with_most_depDelay(argv[1])
    print('{} flight that expierences the most departure delay'.format(most_dep_delay_flight['FlightNum']))
    
    least_dep_delay_flight = perf.originated_flight_with_least_depDelay(argv[1])
    print('{} flight that expierences the least departure delay'.format(least_dep_delay_flight['FlightNum']))

    avg_arrival_delay_carrier = perf.carrier_with_avg_arrDelay(argv[1])
    print('{} average arrival delay {}'.format(avg_arrival_delay_carrier['UniqueCarrier'], avg_arrival_delay_carrier['avgarrDelay']))
    
    avg_depart_delay_carrier = perf.carrier_with_avg_depDelay(argv[1])
    print('{} average departure delay {}'.format(avg_depart_delay_carrier['UniqueCarrier'] , avg_depart_delay_carrier['avgdepDelay']))


    
if __name__ == '__main__':
    main(sys.argv[1:])
