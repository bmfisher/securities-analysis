CREATE TABLE IF NOT EXISTS company 
(
    company_id  SERIAL PRIMARY KEY,
    full_name   varchar(40) NOT NULL,
    ticker      varchar(8) NOT NULL,
    UNIQUE(full_name, ticker)
);