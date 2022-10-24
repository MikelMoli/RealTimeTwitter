import sys
sys.path.append('./')

import findspark
findspark.init()

import nltk
import textblob
from textblob import TextBlob

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


import settings

class StreamConsumer:

    def __init__(self):
        self.tweet_schema = StructType([ 
                                    StructField("id", StringType(), True),
                                    StructField("author_id", StringType(), True),
                                    StructField("text", StringType(), True),
                                    StructField("in_reply_to_user_id", StringType(), True)
                                    ])

    def preprocess_tweets(self, stream):
        tweets = stream.withColumn('message', F.from_json(stream.value, self.tweet_schema)).select('message.*')
        tweets = tweets.select(F.explode(F.split(tweets.text, "t_end")).alias("text"))
        tweets = tweets.na.replace('', None)
        tweets = tweets.na.drop()
        tweets = tweets.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
        tweets = tweets.withColumn('text', F.regexp_replace('text', '@\w+', ''))
        tweets = tweets.withColumn('text', F.regexp_replace('text', '#', ''))
        tweets = tweets.withColumn('text', F.regexp_replace('text', 'RT', ''))
        tweets = tweets.withColumn('text', F.regexp_replace('text', ':', ''))
        return tweets


    def polarity_detection(self, text):
        # TODO: Replace with huggingface model
        from textblob import TextBlob
        return TextBlob(text).sentiment.polarity


    def subjectivity_detection(self, text):
        # TODO: Replace with huggingface model
        from textblob import TextBlob
        return TextBlob(text).sentiment.subjectivity

    def text_classification(self, words):
        # polarity detection
        polarity_detection_udf = F.udf(self.polarity_detection, StringType())
        words = words.withColumn("polarity", polarity_detection_udf("text"))
        # subjectivity detection
        subjectivity_detection_udf = F.udf(self.subjectivity_detection, StringType())
        words = words.withColumn("subjectivity", subjectivity_detection_udf("text"))
        return words

    def run(self):

        spark = SparkSession \
                    .builder \
                    .appName("TwitterStream") \
                    .getOrCreate()


        tweet_stream = spark.readStream \
                                .format('socket') \
                                .option('host', settings.SOCKET['HOST']) \
                                .option('port', settings.SOCKET['PORT']) \
                                .load()
        
        preprocessed_tweets = self.preprocess_tweets(tweet_stream)

        sentiment_data = self.text_classification(preprocessed_tweets)
        sentiment_data = sentiment_data.repartition(1)

        query = sentiment_data \
                .writeStream \
                .format("console") \
                .outputMode('append') \
                .trigger(processingTime='10 seconds') \
                .start()

        query.awaitTermination()





if __name__ == '__main__':
    consumer = StreamConsumer()
    consumer.run()