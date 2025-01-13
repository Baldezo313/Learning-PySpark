import os
import sys
import glob
from os.path import abspath
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import findspark
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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

spark = SparkSession \
	.builder \
	.appName("StructuredNetworkWordCount") \
	.getOrCreate()


 # Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark\
	.readStream\
	.format('socket')\
	.option('host', 'localhost')\
	.option('port', 9999)\
	.load()

# Split the lines into words
words = lines.select(
	explode(
    	split(lines.value, ' ')
	).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count()


# Start running the query that prints the running counts to the console
query = wordCounts\
	.writeStream\
	.outputMode('complete')\
	.format('console')\
	.start()

# Await Spark Streaming termination
query.awaitTermination()

