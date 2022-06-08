import sys
sys.path.append('./')

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace

import settings

spark = SparkSession \
        .builder \
        .appName("TwitterStream") \
        .getOrCreate()

tweet_stream = spark.readStream \
                    .format('socket') \
                    .option('host', '127.0.0.1') \
                    .option('port', settings.SOCKET['PORT']) \
                    .load()

tweets = tweet_stream.select(
        explode(split(tweet_stream.value, settings.ANALYSIS['TWEET_EOL'])).alias('message')) \
        .withColumn('message', regexp_replace('message', r'http\S+', '')) \
        .withColumn('message', regexp_replace('message', '@\w+', '')) \
        .withColumn('message', regexp_replace('message', '#', '')) \
        .withColumn('message', regexp_replace('message', 'RT', '')) \
        .withColumn('message', regexp_replace('message', ':', ''))


query = tweets \
        .writeStream \
        .format("console") \
        .start()
                
query.awaitTermination()
