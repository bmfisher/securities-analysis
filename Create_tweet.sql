CREATE TABLE IF NOT EXISTS tweet
(
    tweet_id    SERIAL PRIMARY KEY,
    company_id  integer REFERENCES company (company_id) NOT NULL,
    twitter_tweet_id varchar(64) NOT NULL,
    full_text   varchar(2048) NOT NULL,
    post_time   TIMESTAMP NOT NULL,
    sentiment   NUMERIC(8,6) NULL
);