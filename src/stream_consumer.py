import sys
import os
sys.path.append('./')
sys.path.append('./src')

import findspark
findspark.init()

from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import settings

class StreamConsumer:

    def __init__(self):
        self.model_endpoint = 'http://localhost:5000/predict'
        self.tweet_schema = StructType([ 
                                    StructField("id", StringType(), True),
                                    StructField("author_id", StringType(), True),
                                    StructField("text", StringType(), True),
                                    StructField("in_reply_to_user_id", StringType(), True)
                                    ])
        # self.analyzer_controller = AnalyzerController()
        # self.sentiment_analyzer = create_analyzer(task="sentiment", lang="es")
        # self.classifier = pipeline("sentiment-analysis", model='pysentimiento/robertuito-sentiment-analysis')
        # classifier = pipeline("sentiment-analysis", model='robertuito-sentiment-analysis') --> pide token

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


    def sentiment_detection(self, text):
        import requests
        import json
        msg = {'data': text}
        response = requests.post(self.model_endpoint, json=msg)
        sentiment_dict = json.loads(response.text)
        label = sentiment_dict['label']
        score = sentiment_dict['score']
        return label, score

    def text_classification(self, words, sentiment_schema):
        sentiment_detection_udf = F.udf(self.sentiment_detection, sentiment_schema)
        words = words.withColumn("sentiment", sentiment_detection_udf("text"))  \
                     .select(F.col("text"), F.col("sentiment.*"))
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

        # TODO: Seguir mirando cómo hacer para pillar los dos valores del sentiment analysis

        sentiment_schema = StructType([
                                        StructField("label", StringType(), False),
                                        StructField("score", DoubleType(), False)
                                    ])

        sentiment_data = self.text_classification(preprocessed_tweets, sentiment_schema)
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