import os
import sys
import glob
from os.path import abspath
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import findspark

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

# Create a SparkSession (SparkContext is no longer used directly in Spark 3.x)
spark = SparkSession.builder \
    .appName("NetworkWordCount") \
    .master("local[2]") \
    .getOrCreate()

# Create a StreamingContext using the SparkSession
ssc = spark.streams

# Create a DStream that will connect to the stream of input lines from connection to localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Split lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()


