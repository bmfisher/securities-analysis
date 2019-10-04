CREATE TABLE IF NOT EXISTS price 
(
    price_id    SERIAL PRIMARY KEY,
    company_id  integer REFERENCES company (company_id) NOT NULL,
    quote_time  TIMESTAMP NOT NULL,
    price       NUMERIC(12,6) NOT NULL,
    UNIQUE(quote_time, company_id)
)