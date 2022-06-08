# This file should be renamed to settings.py and all the data in API_CREDENTIALS SHOULD BE FILLED WITH YOUR OWN


API_CREDENTIALS = {
        "API_KEY": "YOUR_API_KEY",
        "API_SECRET_KEY": "YOUR_SECRET_API_KEY",
        "BEARER_TOKEN": "YOUR_BEARER_TOKEN",
        "ACCESS_TOKEN": "YOUR_ACCESS_TOKEN",
        "ACCESS_TOKEN_SECRET": "YOUR_SECRET_ACCESS_TOKEN"
        }

ANALYSIS = {
    'TWEET_EOL': '\n', # Not necessary in this example, but it could be used to indicate the end of communication with a different protocol
}

# Only change the SOCKET variable values if you want to set another address or port
SOCKET = {
    'HOST': '127.0.0.1', 
    'PORT': 9000 
}