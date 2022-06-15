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
                    .option('host', settings.SOCKET['HOST']) \
                    .option('port', settings.SOCKET['PORT']) \
                    .load()

tweets = tweet_stream.select(
        explode(split(tweet_stream.value, settings.ANALYSIS['TWEET_EOL'])).alias('message'))


query = tweets \
        .writeStream \
        .format("csv") \
        .option("checkpointLocation", "checkpoint/") \
        .option("path", "output_path/") \
        .outputMode("append") \
        .start()
                
query.awaitTermination()
