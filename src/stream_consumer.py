import sys
import socket

sys.path.append('./')
sys.path.append('./src')

import findspark
findspark.init()
import traceback
from settings import DATABASE_CONNECTION
import psycopg2

import requests
import json
from json.decoder import JSONDecodeError
import re

from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import settings

class StreamConsumer:

    def __init__(self, mode='sequential'):
        self.model_endpoint = 'http://localhost:5000/predict'
        self.tweet_schema = StructType([ 
                                    StructField("id", StringType(), True),
                                    StructField("author_id", StringType(), True),
                                    StructField("text", StringType(), True),
                                    StructField("in_reply_to_user_id", StringType(), True)
                                    ])
        self.mode = mode

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

    def parallel_execution(self):
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

    def sequential_execution(self):
        client_socket = socket.socket()
        client_socket.connect((settings.SOCKET['HOST'], settings.SOCKET['PORT']))
        counter = 0
        while True:
            try:
                data = client_socket.recv(1024).decode('utf-8')
                
                print(f'[RECEIVED DATA] \n "{data}"')
                data = json.loads(data)
                
                data = data['text']
                data = re.sub('@\w+', '', data)
                msg = {'data': data}
                response = requests.post(self.model_endpoint, json=msg)


                sentiment_dict = json.loads(response.text)
                sentiment_dict['tweet'] = data

                connector = psycopg2.connect(host=DATABASE_CONNECTION['host'],
                            dbname=DATABASE_CONNECTION['dbname'],
                            user=DATABASE_CONNECTION['user'],
                            password=DATABASE_CONNECTION['password']
                            )
                with connector.cursor() as cur:
                    try:
                        
                        cur.execute("""INSERT INTO sentiment_tweets(tweet,label,score) VALUES (%(tweet)s, %(label)s, %(score)s)""", sentiment_dict)
                        connector.commit()
                    except psycopg2.DatabaseError as insert_error:
                        print('--------------- EXCEPTION START [SCRAP PAGE COMMENT]: ---------------------')
                        lines = traceback.format_exception(type(insert_error), insert_error, insert_error.__traceback__)
                        print(''.join(lines))
                        print('--------------- EXCEPTION END: ---------------------')
                        pass
                    finally:
                        connector.close()
            except JSONDecodeError as json_error:
                print('Error handling message.')
                continue

if __name__ == '__main__':
    consumer = StreamConsumer(mode='sequential')
    if consumer.mode == 'parallel':
        consumer.parallel_execution()

    elif consumer.mode == 'sequential':
        consumer.sequential_execution()

