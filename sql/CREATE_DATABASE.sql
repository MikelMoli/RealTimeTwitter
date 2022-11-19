CREATE TABLE IF NOT EXISTS sentiment_tweets (
   tweet VARCHAR NOT NULL,
   label VARCHAR(3) NOT NULL,
   score NUMERIC
);