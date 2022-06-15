import sys
from time import sleep
sys.path.append('./')

import tweepy
import settings
import json

import socket

class TwitterStream(tweepy.StreamingClient):
    def __init__(self, application_socket):
        super().__init__(settings.API_CREDENTIALS['BEARER_TOKEN'])
        self.client_socket = application_socket

    def on_tweet(self, tweet):
        print(type(tweet))
        data = {
            "id": tweet.id,
            "user_id": tweet.author_id, 
            "text": tweet.text, 
            "context_annotations": tweet.context_annotations, 
            "created_ts": tweet.created_at,
            "geolocation": tweet.geo,
            "in_reply_to_user_id": tweet.in_reply_to_user_id,
            "lang": tweet.lang,
            "non_public_metrics": tweet.non_public_metrics,
            "organic_metrics": tweet.organic_metrics,
            "possibly_sensitive": tweet.possibly_sensitive,
            "promoted_metrics": tweet.promoted_metrics
            }
        self.client_socket.send((str(data)).encode('utf-8').strip())
        self.client_socket.send(str("\n").encode('utf-8'))


    def on_error(self, status):
        print(status)
        return True

if __name__=='__main__':
    socket = socket.socket()
    socket.bind((settings.SOCKET['HOST'], settings.SOCKET['PORT']))
    socket.listen()

    print(f"Socket listening in {settings.SOCKET['HOST']}:{settings.SOCKET['PORT']} and waiting for Spark")

    application_socket, address = socket.accept()
    application_socket.setblocking(False)
    print("Client connected!")
    streaming_client = TwitterStream(application_socket)
    streaming_client.add_rules(tweepy.StreamRule('"Pedro Sanchez" or VOX lang:es')) # This creates a filtered stream
    streaming_client.filter() # This creates a filtered stream


    # IF STREAM IS NOT WORKING, DELETE checkpoint AND output_path FOLDERS AND RERUN BOTH SCRIPTS
    
