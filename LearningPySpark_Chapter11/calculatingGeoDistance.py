import utilities.geoCalc as geo
from utilities.converters import metricImperial

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func

import os
import sys
import glob
from os.path import abspath
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import findspark
from pyspark.sql.functions import explode, split

# Set up environment variables for Spark and Java
os.environ["SPARK_HOME"] = "C:/Program Files/spark-3.5.4-bin-hadoop3"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jre1.8.0_431"
os.environ['HADOOP_HOME'] = 'C:/Program Files/hadoop-3.4.0'

spark_python = os.path.join(os.environ.get('SPARK_HOME', None), 'python')
py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
graphf = glob.glob(os.path.join(spark_python, 'graphframes.zip'))[0]
sys.path[:0] = [spark_python, py4j]
sys.path[:0] = [spark_python, graphf]
os.environ['PYTHONPATH'] = py4j + os.pathsep + graphf

# Initialize findspark
import findspark
findspark.init()
findspark.find()

# Create a Spark session
spark = SparkSession.builder \
    .appName("Submit Spark Job") \
    .getOrCreate()

def geoEncode(spark):
    # read the data in
    uber = spark.read.csv(
        'uber_data_nyc_2016-06_3m_partitioned.csv', 
        header=True, 
        inferSchema=True
        )\
        .repartition(4) \
        # .select('VendorID','tpep_pickup_datetime', 'pickup_longitude', 'pickup_latitude','dropoff_longitude','dropoff_latitude','total_amount')

    # prepare the UDFs
    getDistance = func.udf(
        lambda lat1, long1, lat2, long2: 
            geo.calculateDistance(
                (lat1, long1),
                (lat2, long2)
            )
        )

    convertMiles = func.udf(lambda m: 
        metricImperial.convert(str(m) + ' mile', 'km'))

    # create new columns
    uber = uber.withColumn(
        'miles', 
            getDistance(
                func.col('pickup_latitude'),
                func.col('pickup_longitude'), 
                func.col('dropoff_latitude'), 
                func.col('dropoff_longitude')
            )
        )

    uber = uber.withColumn(
        'kilometers', 
        convertMiles(func.col('miles')))

    # print 10 rows
    # uber.show(10)

    # save to csv (partitioned)
    uber.write.csv(
        'uber_data_nyc_2016-06_new.csv',
        mode='overwrite',
        header=True,
        compression='gzip'
    )

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('CalculatingGeoDistances') \
        .getOrCreate()

    print('Session created')

    try:
        geoEncode(spark)

    finally:
        spark.stop()