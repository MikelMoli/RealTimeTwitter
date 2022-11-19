. venv/bin/activate
flask run --host=0.0.0.0 &
python src/twitter_stream.py
python src/stream_consumer.py