CREATE TABLE IF NOT EXISTS search
(
    search_id SERIAL PRIMARY KEY,
    company_id integer REFERENCES company (company_id) NOT NULL,
    search_text varchar(64) NOT NULL
);