import sys
import json
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
        
        tweet_text = tweet.text.replace('\n', ' ')
        data = {
                "id": tweet.id,
                "author_id": tweet.author_id, 
                "text": tweet_text
            }

        print(data)
        if tweet.in_reply_to_user_id is not None:
            data['in_reply_to_user_id'] = tweet.in_reply_to_user_id
        else:
            data['in_reply_to_user_id'] = 'null'    

        self.client_socket.send((json.dumps(data)).encode('utf-8').strip())
        # TODO: Cuando hay un \n peta así que hay que cambiarlo
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
    streaming_client.delete_rules(['1592941675076354050'])
    if streaming_client.running is True:
        streaming_client.disconnect()

    # streaming_client.add_rules(tweepy.StreamRule('(luz or precio or electricidad or factura)')) # This creates a filtered stream
    print(streaming_client.get_rules()) # Check if the rules are the expected ones
    streaming_client.add_rules(tweepy.StreamRule('lang:es -is:quote -has:mentions -is:retweet -is:reply (ayuso OR sanidad OR transporte OR huelga)')) # This creates a filtered stream
    # streaming_client.add_rules(tweepy.StreamRule('lang:es'))
    print(streaming_client.get_rules())

    # streaming_client.filter(tweet_fields=["referenced_tweets", "author_id", "created_at"] ) # This creates a filtered stream
    streaming_client.filter(tweet_fields=["id", "author_id", "text"] ) # This creates a filtered stream


    # IF STREAM IS NOT WORKING, DELETE checkpoint AND output_path FOLDERS AND RERUN BOTH SCRIPTS
    
