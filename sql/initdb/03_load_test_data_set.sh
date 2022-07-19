#!/bin/bash
set -e

SCHEMA_DATA="d_silos"
SCHEMA_REPORT="dashboards"


psql -v ON_ERROR_STOP=1 --dbname dwarehouse <<-EOSQL

-- ===========================================================
--   TESTS
-- ===========================================================

-- =====================
-- INSERT INTO $SCHEMA_DATA.catalog_asset
-- =====================

INSERT INTO $SCHEMA_DATA.catalog_asset (name, metadata, tags)
VALUES
(
'equity_msft', 
'{"zip": "98052-6399",
 "sector": "Technology",
 "fullTimeEmployees": 181000,
 "longBusinessSummary": "Microsoft Corporation develops..",
 "city": "Redmond",
 "phone": "425 882 8080",
 "state": "WA",
 "country": "United States",
 "companyOfficers": [],
 "website": "https://www.microsoft.com",
 "industry": "Software—Infrastructure",
 "financialCurrency": "USD",
 "exchange": "NMS",
 "shortName": "Microsoft Corporation",
 "longName": "Microsoft Corporation",
 "quoteType": "EQUITY",
 "symbol": "MSFT",
 "market": "us_market",
 "data_producer": "unknown",
 "data_provider": "Yahoo Finance"
	}'::jsonb,
'["equity","it"]'
),
(
'equity_aapl',
'{"zip": "95014",
 "sector": "Technology",
 "fullTimeEmployees": 154000,
 "longBusinessSummary": "Apple Inc. designs, manufactur..",
 "city": "Cupertino",
 "phone": "408 996 1010",
 "state": "CA",
 "country": "United States",
 "companyOfficers": [],
 "website": "https://www.apple.com",
 "industry": "Consumer Electronics",
 "financialCurrency": "USD",
 "exchange": "NMS",
 "shortName": "Apple Inc.",
 "longName": "Apple Inc.",
 "quoteType": "EQUITY",
 "symbol": "AAPL",
 "market": "us_market",
 "data_producer": "unknown",
 "data_provider": "Yahoo Finance"	
	}'::jsonb,
'["equity","it"]'
);


-- SELECT *
-- FROM $SCHEMA_DATA.catalog_asset;

-- SELECT ID
-- FROM $SCHEMA_DATA.CATALOG_ASSET
-- WHERE METADATA ->> 'symbol' = 'AAPL';


-- =====================
-- INSERT INTO $SCHEMA_DATA.catalog_attribute
-- =====================

INSERT INTO $SCHEMA_DATA.catalog_attribute (name, metadata)
VALUES
('price;day;open', '{"yfinance_api":"Open"}'),
('price;day;close', '{"yfinance_api":"Close"}'),
('price;day;high', '{"yfinance_api":"High"}'),
('price;day;low', '{"yfinance_api":"Low"}'),
('volume;day;traded', '{"yfinance_api":"Volume"}');

-- SELECT *
-- FROM $SCHEMA_DATA.catalog_attribute;

-- SELECT ID
-- FROM $SCHEMA_DATA.CATALOG_ATTRIBUTE
-- WHERE METADATA ->> 'yfinance_api' = 'Close';


-- =====================
-- INSERT INTO $SCHEMA_DATA.catalog_customer
-- =====================

INSERT INTO $SCHEMA_DATA.catalog_customer (name, metadata)
VALUES
('user_1@gmail.com', '{"name_given":"Alpha"}'),
('user_2@gmail.com', '{"name_given":"Beta"}'),
('user_3@gmail.com', '{"name_given":"Gamma"}'),
('user_4@gmail.com', '{"name_given":"Epsilon"}'),
('user_5@gmail.com', '{"name_given":"Zeta"}');

-- SELECT *
-- FROM $SCHEMA_DATA.catalog_customer;




-- =====================
-- Asset prices
-- =====================

-- -- GENERATE SMALL SERIES:
insert into $SCHEMA_REPORT.prices(datetime, asset_id, price)
values
('2022-01-02 18:00:00', 2, 143),
('2022-01-03 18:00:00', 2, 144),
('2022-01-04 18:00:00', 2, 145);

-- -- GENERATE SMALL SERIES OF UPDATES:
insert into $SCHEMA_REPORT.prices(datetime, asset_id, price)
values
('2022-01-05 18:00:00', 2, 143),
('2022-01-05 18:00:00', 2, 144),
('2022-01-05 18:00:00', 2, 145);

-- -- GENERATE BIG SERIES (2020 rows) OF 10minutes interval PUBLISHED DATA:
INSERT INTO $SCHEMA_REPORT.prices(datetime, asset_id, price)
SELECT ts, 1, (1.0 + 1.0 * cos(rownum * 6.28/180))*50 + random()*50 + 50
FROM generate_series('2022-02-01'::timestamp,'2022-02-15'::timestamp,INTERVAL '10 minute') WITH ORDINALITY AS t(ts,rownum);


-- CALL PROCEDURE:
-- call $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target();

-- CALL FUNCTION:
-- SELECT $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target();


-- SELECT *
-- FROM $SCHEMA_DATA.silo_asset_price_scalar_CACHE
-- ORDER BY ID DESC
-- LIMIT 10;


-- SELECT *
-- FROM $SCHEMA_DATA.silo_asset_price_scalar
-- ORDER BY 
-- 	published_at_cet DESC,
-- 	asset_id,
-- 	attribute_id,
-- 	data_validation ASC
-- LIMIT 5;

-- SELECT * FROM $SCHEMA_REPORT.prices;

-- SELECT * FROM $SCHEMA_REPORT.asset_price_scalar_hourly_avg


-- Create record with wrong attribute and asset names:
insert into $SCHEMA_DATA.silo_asset_price_scalar_cache( 
	asset, 
	attribute,
	value, 
	published_at_cet, 
	status, 
	created_by_name)
select 
	'this is wrong value',
	'this is wrong value',
	value, 
	published_at_cet, 
	'new', 
	'root' 
from $SCHEMA_DATA.silo_asset_price_scalar_cache where id = (select min(id) from $SCHEMA_DATA.silo_asset_price_scalar_cache);

-- Create duplicate record:
insert into $SCHEMA_DATA.silo_asset_price_scalar_cache( 
	asset, 
	attribute,
	value, 
	published_at_cet, 
	status, 
	created_by_name)
select 
	asset, 
	attribute,
	value, 
	published_at_cet, 
	'new', 
	'root' 
from $SCHEMA_DATA.silo_asset_price_scalar_cache where id = (select min(id) from $SCHEMA_DATA.silo_asset_price_scalar_cache);



-- -- TEST UPDATE REJECT
-- update $SCHEMA_REPORT.prices
-- set price = 200
-- where 
-- 	1=1
-- 	and datetime = (select max(datetime) from $SCHEMA_REPORT.prices);

-- -- TEST DELETE REJECT
-- delete from $SCHEMA_REPORT.prices
-- where 
-- 	1=1
-- 	and datetime = (select max(datetime) from $SCHEMA_REPORT.prices);





-- =====================
-- Portfolio return
-- =====================

-- -- GENERATE SMALL SERIES:
INSERT INTO $SCHEMA_REPORT.PORTFOLIO_RETURN (
	date, 
	CUSTOMER_ID,
	PORTFOLIO_RETURN)
VALUES
('2022-01-10', 1, -0.0365),
('2022-01-10', 2, 0.001256),
('2022-01-11', 1, 0.00024),
('2022-01-11', 2, -0.01756),
('2022-01-12', 1, 0.01036),
('2022-01-12', 2, 0.01046);

-- -- GENERATE SMALL SERIES OF UPDATES:
INSERT INTO $SCHEMA_REPORT.PORTFOLIO_RETURN (
	date, 
	CUSTOMER_ID,
	PORTFOLIO_RETURN)
VALUES
('2022-01-13', 1, 0.01),
('2022-01-13', 1, 0.02),
('2022-01-13', 1, 0.03);

-- -- GENERATE BIG SERIES (50 rows) OF daily interval PUBLISHED DATA:
INSERT INTO $SCHEMA_REPORT.PORTFOLIO_RETURN (
	date, 
	CUSTOMER_ID,
	PORTFOLIO_RETURN)
SELECT ts, 1, (1.0 * cos(rownum * 6.28/180))*5*random()
FROM generate_series('2022-02-01'::timestamp,'2022-03-31'::timestamp,INTERVAL '1 day') WITH ORDINALITY AS t(ts,rownum);


-- SELECT *
-- FROM $SCHEMA_DATA.silo_portfolio_return_CACHE
-- ORDER BY
-- 	published_at DESC,
-- 	customer_id
-- 	;

-- SELECT *
-- FROM $SCHEMA_DATA.silo_portfolio_return
-- ORDER BY
-- 	published_at DESC,
-- 	customer_id,
-- 	data_validation
-- ;


-- SELECT *
-- FROM $SCHEMA_REPORT.PORTFOLIO_RETURN;

-- SELECT *
-- FROM $SCHEMA_REPORT.PORTFOLIO_RETURN_MONTHLY;





-- -- TEST UPDATE REJECT
-- UPDATE $SCHEMA_REPORT.PORTFOLIO_RETURN
-- SET PORTFOLIO_RETURN = 200
-- WHERE 1 = 1
-- 	AND date = (select max(date) from $SCHEMA_REPORT.PORTFOLIO_RETURN);

-- -- TEST DELETE REJECT
-- DELETE
-- FROM $SCHEMA_REPORT.PORTFOLIO_RETURN
-- WHERE 1 = 1
-- 	AND date = (select max(date) from $SCHEMA_REPORT.PORTFOLIO_RETURN);





EOSQL

# FIXME: without this sleep db will not finish import because entrypoint.sh of
#        official postgres docker image shutdowns server after custom entrypoints scripts
sleep 1