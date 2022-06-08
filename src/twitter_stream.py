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
        print(tweet.id)
        self.client_socket.send(str(tweet).encode('utf-8').strip())
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
    streaming_client = TwitterStream(application_socket)
    streaming_client.sample()
