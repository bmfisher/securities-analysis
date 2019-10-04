CREATE TABLE IF NOT EXISTS tweet
(
    tweet_id    SERIAL PRIMARY KEY,
    company_id  integer REFERENCES company (company_id) NOT NULL,
    full_text   varchar(280) NOT NULL,
    post_time   TIMESTAMP NOT NULL,
    sentiment   NUMERIC(8,6) NOT NULL
);