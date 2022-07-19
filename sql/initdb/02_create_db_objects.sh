#!/bin/bash
set -e

SCHEMA_DATA="d_silos"
SCHEMA_REPORT="dashboards"


psql -v ON_ERROR_STOP=1 --dbname dwarehouse <<-EOSQL


-- ===========================================================
--   CREATE SCHEMAS
-- ===========================================================

CREATE SCHEMA IF NOT EXISTS $SCHEMA_DATA;
CREATE SCHEMA IF NOT EXISTS $SCHEMA_REPORT;



-- ===========================================================
--   GRANT PRIVILEGES
-- ===========================================================

-- =====================
-- GRANT ON DB:
-- =====================
GRANT CONNECT ON DATABASE $DB_NAME 
TO $DB_GROUP_W, $DB_GROUP_R;

-- =====================
-- GRANT ON SCHEMA:
-- =====================
GRANT USAGE ON SCHEMA $SCHEMA_DATA, $SCHEMA_REPORT 
TO $DB_GROUP_W, $DB_GROUP_R;

-- =====================
-- GRANT DEFAULT ON TABLES:
-- =====================
ALTER DEFAULT PRIVILEGES IN SCHEMA $SCHEMA_DATA , $SCHEMA_REPORT 
GRANT SELECT ON TABLES 
TO $DB_GROUP_W, $DB_GROUP_R;


ALTER DEFAULT PRIVILEGES IN SCHEMA $SCHEMA_DATA , $SCHEMA_REPORT 
GRANT INSERT, UPDATE ON TABLES 
TO $DB_GROUP_W;



-- =====================
-- GRANT DEFAULT ON SEQUENCES:
-- =====================
-- For sequences, SELECT privilege also allows the use of the currval function
-- For sequences, USAGE privilege allows the use of the currval and nextval functions.
ALTER DEFAULT PRIVILEGES IN SCHEMA $SCHEMA_DATA , $SCHEMA_REPORT 
GRANT SELECT ON SEQUENCES 
TO $DB_GROUP_R;

ALTER DEFAULT PRIVILEGES IN SCHEMA $SCHEMA_DATA , $SCHEMA_REPORT 
GRANT USAGE ON SEQUENCES 
TO $DB_GROUP_W;


-- =====================
-- GRANT DEFAULT ON FUNCTIONS:
-- =====================
ALTER DEFAULT PRIVILEGES IN SCHEMA $SCHEMA_DATA , $SCHEMA_REPORT 
GRANT EXECUTE ON FUNCTIONS TO $DB_GROUP_W;


-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA $SCHEMA_REPORT TO $DB_GROUP_W;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA $SCHEMA_REPORT TO $DB_GROUP_W;

-- GRANT SELECT ON ALL TABLES IN SCHEMA $SCHEMA_REPORT TO $DB_GROUP_R;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA $SCHEMA_REPORT TO $DB_GROUP_R;


-- ===========================================================
--   CREATE TABLES
-- ===========================================================

-- =====================
-- Catalog with assets:
-- =====================
DROP TABLE IF EXISTS $SCHEMA_DATA.catalog_asset CASCADE;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.catalog_asset (
	id bigint PRIMARY KEY generated always as identity,
	name text NOT NULL UNIQUE,
	metadata jsonb,
	tags jsonb
);
COMMENT ON TABLE $SCHEMA_DATA.catalog_asset IS 'Natural catalog of liquid assets like cash, stocks, bonds, mutual funds, bank deposits etc';
--SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.catalog_asset'::regclass);


-- =====================
-- Catalog with asset attributes:
-- =====================
DROP TABLE IF EXISTS $SCHEMA_DATA.catalog_attribute CASCADE;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.catalog_attribute (
	id bigint PRIMARY KEY generated always as identity,
	name text not null unique,
	metadata jsonb,
	tags jsonb
);
COMMENT ON TABLE $SCHEMA_DATA.catalog_attribute IS 'Natural catalog of asset attributes like open price, closing price, high price, low price, voluem traded etc';
-- SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.catalog_attribute'::regclass);


-- =====================
-- Catalog with customers:
-- =====================
DROP TABLE IF EXISTS $SCHEMA_DATA.catalog_customer CASCADE;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.catalog_customer (
	id bigint PRIMARY KEY generated always as identity,
	name text not null unique,
	metadata jsonb,
	tags jsonb
);
COMMENT ON TABLE $SCHEMA_DATA.catalog_customer IS 'Catalog with customer''s information';
-- SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.catalog_customer'::regclass);


-- =====================
-- Asset attribute publications:
-- =====================

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar CASCADE;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar (
	id bigint GENERATED ALWAYS AS IDENTITY,
	asset_id bigint not null REFERENCES $SCHEMA_DATA.catalog_asset (id) ON DELETE CASCADE ON UPDATE CASCADE,
	attribute_id bigint not null REFERENCES $SCHEMA_DATA.catalog_attribute (id) ON DELETE NO ACTION ON UPDATE CASCADE,
	-- recommendation: avoide to use float64 (corresponds to PostgreSQL "double precision" data type). 
	-- Reasoning: "double precision" is inexact (float type). While float type is good for some scientific reseaches
	-- it's creating the problems and confusions in financial applications, especially when you check for price equality. 
	-- Use "numeric" data type instead:
	value numeric,
	published_at_cet timestamp not null,
	created_by text NOT NULL DEFAULT current_user,
	created_at_cet TIMESTAMPTZ DEFAULT current_timestamp,
	data_validation int default 0,
	
	primary key(id, published_at_cet)
	
) PARTITION BY RANGE (published_at_cet);

COMMENT ON TABLE $SCHEMA_DATA.silo_asset_price_scalar IS 'Scalar values for assets attributes like day open price for stock, day close price for stock etc';
--SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.silo_asset_price_scalar'::regclass);


CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2018_and_ealier
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM (minvalue) TO ('2019-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2019
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2019-01-01 00:00:00') TO ('2020-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2020
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2020-01-01 00:00:00') TO ('2021-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2021
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2022-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2022
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2022-01-01 00:00:00') TO ('2023-01-01 00:00:00');
 
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2023
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2023-01-01 00:00:00') TO ('2024-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2024
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2025
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_2026
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar
FOR VALUES FROM ('2026-01-01 00:00:00') TO (maxvalue);


-- =====================
-- Temporary storage for records to be inserted into asset_price_scalar:
-- =====================

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache (
	id bigint GENERATED ALWAYS AS IDENTITY,
	asset text not null,
	attribute text not null,
	value numeric,
	published_at_cet timestamp not null,
	created_by_name text,
	created_by_ip_address inet DEFAULT inet_client_addr(),
	created_at_cet TIMESTAMPTZ DEFAULT current_timestamp,
	rotating_period int not null DEFAULT (extract(isodow from statement_timestamp() )),
	status text,
	msg text,
	
	primary key (id, rotating_period)
) partition by list (rotating_period);


COMMENT ON TABLE $SCHEMA_DATA.silo_asset_price_scalar_cache IS 'Storage for temporary records that will be sanitized before actual INSERT into asset_price_scalar table';
--SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.silo_asset_price_scalar_cache'::regclass);


DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_mon;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_mon
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (1);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_tue;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_tue
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (2);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_wed;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_wed
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (3);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_thu;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_thu
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (4);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_fri;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_fri
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (5);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_sat;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_sat
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (6);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_sun;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_sun
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache FOR VALUES IN (7);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_default;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_default
PARTITION OF $SCHEMA_DATA.silo_asset_price_scalar_cache DEFAULT;


DROP FUNCTION IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_truncate_old_partition CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_DATA.silo_asset_price_scalar_cache_truncate_old_partition()
	RETURNS TRIGGER
	LANGUAGE 'plpgsql' strict
AS
\$func\$
DECLARE
	_suffix_insert int := (NEW.rotating_period);
	_dow_index_truncate int := ((NEW.rotating_period+1) % 7)::int;
	_suffix_truncate text := (array['mon','tue','wed','thu','fri','sat','sun'])[_dow_index_truncate];
	_rows_count int;
BEGIN
	EXECUTE 'select count(*) FROM $SCHEMA_DATA.silo_asset_price_scalar_cache_' || _suffix_truncate INTO _rows_count;
	IF coalesce(_rows_count, 0) > 0 THEN
		SET lock_timeout = '1s';
		EXECUTE 'truncate $SCHEMA_DATA.silo_asset_price_scalar_cache_'|| _suffix_truncate ;
	END IF;
	RETURN null;
END;
\$func\$;


DROP TRIGGER IF EXISTS after_insert_001 ON $SCHEMA_DATA.silo_asset_price_scalar_cache;
CREATE TRIGGER after_insert_001
	AFTER INSERT 
	ON $SCHEMA_DATA.silo_asset_price_scalar_cache
FOR EACH ROW EXECUTE FUNCTION $SCHEMA_DATA.silo_asset_price_scalar_cache_truncate_old_partition();


-- =====================
-- Portfolio return publications:
-- =====================

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return CASCADE;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return (
	id bigint GENERATED ALWAYS AS IDENTITY,
	customer_id bigint not null REFERENCES $SCHEMA_DATA.catalog_customer (id) ON DELETE CASCADE ON UPDATE CASCADE,
	portfolio_return numeric,
	published_at date not null,
	created_by text NOT NULL DEFAULT current_user,
	created_at_cet TIMESTAMPTZ DEFAULT current_timestamp,
	data_validation int default 0,
	
	primary key(id, published_at)
	
) PARTITION BY RANGE (published_at);

COMMENT ON TABLE $SCHEMA_DATA.silo_portfolio_return IS 'Customer''s portfolio daily return';
--SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.silo_portfolio_return'::regclass);


CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2018_and_ealier
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM (MINVALUE) TO ('2019-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2019
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2020
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2021
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2022
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
 
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2023
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2024
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2025
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_2026
PARTITION OF $SCHEMA_DATA.silo_portfolio_return
FOR VALUES FROM ('2026-01-01') TO (maxvalue);



-- =====================
-- Temporary storage for records to be inserted into portfolio_return:
-- =====================

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache (
	id bigint GENERATED ALWAYS AS IDENTITY,
	customer_id bigint not null,
	portfolio_return numeric,
	published_at date not null,
	created_by_name text,
	created_by_ip_address inet DEFAULT inet_client_addr(),
	created_at_cet TIMESTAMPTZ DEFAULT current_timestamp,
	rotating_period int not null DEFAULT (extract(isodow from statement_timestamp() )),
	status text,
	msg text,
	
	primary key (id, rotating_period)
) partition by list (rotating_period);

COMMENT ON TABLE $SCHEMA_DATA.silo_portfolio_return_cache IS 'Storage for temporary records that will be sanitized before actual INSERT into portfolio_return table';
--SELECT OBJ_DESCRIPTION('$SCHEMA_DATA.silo_portfolio_return_cache'::regclass);


DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_mon;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_mon
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (1);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_tue;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_tue
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (2);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_wed;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_wed
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (3);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_thu;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_thu
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (4);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_fri;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_fri
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (5);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_sat;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_sat
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (6);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_sun;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_sun
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache FOR VALUES IN (7);

DROP TABLE IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_default;
CREATE TABLE IF NOT EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_default
PARTITION OF $SCHEMA_DATA.silo_portfolio_return_cache DEFAULT;


DROP FUNCTION IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_truncate_old_partition CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_DATA.silo_portfolio_return_cache_truncate_old_partition()
	RETURNS TRIGGER
	LANGUAGE 'plpgsql' strict
AS
\$func\$
DECLARE
	_suffix_insert int := (NEW.rotating_period);
	_dow_index_truncate int := ((NEW.rotating_period+1) % 7)::int;
	_suffix_truncate text := (array['mon','tue','wed','thu','fri','sat','sun'])[_dow_index_truncate];
	_rows_count int;
BEGIN
	EXECUTE 'select count(*) FROM $SCHEMA_DATA.silo_portfolio_return_cache_' || _suffix_truncate INTO _rows_count;
	IF coalesce(_rows_count, 0) > 0 THEN
		SET lock_timeout = '1s';
		EXECUTE 'truncate $SCHEMA_DATA.silo_portfolio_return_cache_'|| _suffix_truncate ;
	END IF;
	RETURN null;
END;
\$func\$;


DROP TRIGGER IF EXISTS after_insert_001 ON $SCHEMA_DATA.silo_portfolio_return_cache;
CREATE TRIGGER after_insert_001
	AFTER INSERT 
	ON $SCHEMA_DATA.silo_portfolio_return_cache
FOR EACH ROW EXECUTE FUNCTION $SCHEMA_DATA.silo_portfolio_return_cache_truncate_old_partition();




-- ===========================================================
--   CREATE VIEWS
-- ===========================================================


-- =====================
-- Mirrow asset_price_scalar but with valid records only (data_validation = 0):
-- =====================

DROP VIEW IF EXISTS $SCHEMA_REPORT.asset_price_scalar;
CREATE VIEW $SCHEMA_REPORT.asset_price_scalar
AS
SELECT 
	catalog_asset.name AS asset_name
	,catalog_attribute.name AS attribute_name
	,value
	,published_at_cet
FROM $SCHEMA_DATA.silo_asset_price_scalar 
LEFT JOIN $SCHEMA_DATA.catalog_asset on catalog_asset.id = silo_asset_price_scalar.asset_id
LEFT JOIN $SCHEMA_DATA.catalog_attribute on catalog_attribute.id = silo_asset_price_scalar.attribute_id
WHERE
	1=1
	AND data_validation = 0
ORDER BY 
	published_at_cet desc, 
	asset_name, 
	attribute_name
;


-- =====================
-- Get unique attributes for each asset:
-- =====================
DROP VIEW IF EXISTS $SCHEMA_REPORT.asset_attribute;
CREATE VIEW $SCHEMA_REPORT.asset_attribute
AS
SELECT 
	distinct 
	asset_name,
	attribute_name
FROM $SCHEMA_REPORT.asset_price_scalar
ORDER BY 
	asset_name, 
	attribute_name
;

-- =====================
-- Get asset metadata:
-- =====================
DROP VIEW IF EXISTS $SCHEMA_REPORT.asset_metadata;
CREATE VIEW $SCHEMA_REPORT.asset_metadata
AS
SELECT 
	id,
	name,
	METADATA ->> 'symbol' AS symbol,
	METADATA ->> 'quoteType' AS quoteType,
	METADATA ->> 'financialCurrency' AS financialCurrency,
	METADATA ->> 'sector' AS sector,
	METADATA ->> 'exchange' AS exchange,
	METADATA ->> 'shortName' AS shortName,
	METADATA ->> 'data_producer' AS data_producer,
	METADATA ->> 'data_provider' AS data_provider,
	tags
FROM $SCHEMA_DATA.catalog_asset
ORDER BY 
	name
;



-- =====================
-- Mirrow asset_price_scalar but with valid records only (data_validation = 0):
-- =====================

-- Write a query that yields a time series of the hourly average price per asset 
-- (of course, only the most up-to-date records should be taken into account).
DROP VIEW IF EXISTS $SCHEMA_REPORT.asset_price_scalar_hourly_avg;
CREATE VIEW $SCHEMA_REPORT.asset_price_scalar_hourly_avg
AS
SELECT 
	asset_name
	,attribute_name
	,round(avg(value), 6) as value
	,date_trunc('hour', published_at_cet) as published_at_cet
FROM $SCHEMA_REPORT.asset_price_scalar
GROUP BY 
	 asset_name
	,attribute_name
	,date_trunc('hour', published_at_cet)
ORDER BY 
	published_at_cet desc, 
	asset_name, 
	attribute_name
;


-- =====================
-- View for asset Day Close Price only:
-- =====================

DROP VIEW IF EXISTS $SCHEMA_REPORT.prices;
CREATE VIEW $SCHEMA_REPORT.prices
AS
SELECT
-- 	id,
	published_at_cet as datetime
	,asset_id
	,value as price
	,created_at_cet::timestamp as insertion_timestamp
FROM $SCHEMA_DATA.silo_asset_price_scalar
WHERE
	1=1
	-- get only EoD Close prices:
	AND attribute_id = (SELECT id FROM $SCHEMA_DATA.catalog_attribute WHERE name = 'price;day;close')
	-- get only valid prices (filter out Null, previous versions of the asset price):
	AND data_validation = 0
ORDER BY 
	published_at_cet desc, 
	asset_id
;




-- =====================
-- VIEW for Portfolio return publications:
-- =====================
DROP VIEW IF EXISTS $SCHEMA_REPORT.portfolio_return;
CREATE VIEW $SCHEMA_REPORT.portfolio_return
AS
SELECT 
	id,
	customer_id,
	portfolio_return,
	published_at AS date
FROM $SCHEMA_DATA.silo_portfolio_return
WHERE
	1=1
	AND data_validation = 0
ORDER BY 
	published_at DESC, 
	customer_id
;



-- =====================
-- VIEW for Monthly Portfolio return:
-- =====================

-- thanks:Karl Bartel https://stackoverflow.com/questions/4486973/why-is-there-no-product-aggregate-function-in-sql
DROP FUNCTION IF EXISTS $SCHEMA_REPORT.product_sfunc CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_REPORT.product_sfunc(state numeric, factor numeric)
RETURNS numeric 
AS \$\$
    SELECT \$1 * \$2
\$\$ 
LANGUAGE sql;


CREATE AGGREGATE $SCHEMA_REPORT.product (
    sfunc = $SCHEMA_REPORT.product_sfunc,
    basetype = numeric,
    stype = numeric,
    initcond = '1'
);



DROP VIEW IF EXISTS $SCHEMA_REPORT.portfolio_return_monthly;
CREATE VIEW $SCHEMA_REPORT.portfolio_return_monthly
AS
SELECT 
	customer_id,
	round($SCHEMA_REPORT.product(portfolio_return + 1) - 1, 6) AS portfolio_return,
	date_trunc('month', published_at)::date AS date
FROM $SCHEMA_DATA.silo_portfolio_return
WHERE
	1=1
	AND data_validation = 0
GROUP BY
	customer_id,
	date_trunc('month', published_at)
ORDER BY 
	date DESC, 
	customer_id
;






-- ===========================================================
--   CREATE INSERT AUTO REDIRECTION FROM SQL VIEW via CACHED TABLE to TARGET TABLE
-- ===========================================================

-- =====================
-- redirect INSERT FORM SQL VIEW $SCHEMA_REPORT.prices TO TABLE $SCHEMA_DATA.silo_asset_price_scalar_cache
-- =====================

-- DROP CASCADE: TRIGGER & FUNCTION:
DROP FUNCTION IF EXISTS $SCHEMA_REPORT.prices_insert_redirect_to_cache() CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_REPORT.prices_insert_redirect_to_cache()
	RETURNS trigger
	LANGUAGE 'plpgsql'
	COST 100
-- 	VOLATILE NOT LEAKPROOF
AS \$BODY\$
DECLARE
BEGIN
	IF (TG_OP = 'INSERT') THEN
		INSERT INTO $SCHEMA_DATA.silo_asset_price_scalar_cache(
			asset, 
			attribute, 
			value, 
			published_at_cet,
			created_by_name,
			created_by_ip_address,
			status
		) 
		SELECT 
			coalesce((select name from $SCHEMA_DATA.catalog_asset where id = NEW.asset_id),'not found'), 
			'price;day;close', 
			NEW.price,
			NEW.datetime,
			current_user,
			inet_client_addr(),
			'new'
			; 
		RETURN Null;
	ELSE
		RETURN Null;
	END IF;
END;
\$BODY\$;


-- Trigger: before_modify
DROP TRIGGER IF EXISTS before_modify_001 ON $SCHEMA_REPORT.prices;
CREATE TRIGGER before_modify_001
	INSTEAD OF INSERT OR DELETE OR UPDATE 
	ON $SCHEMA_REPORT.prices
	FOR EACH ROW
	EXECUTE FUNCTION $SCHEMA_REPORT.prices_insert_redirect_to_cache();



-- =====================
-- redirect INSERT FORM SQL VIEW $SCHEMA_REPORT.portfolio_return TO TABLE $SCHEMA_DATA.silo_portfolio_return_cache
-- =====================

-- DROP CASCADE: TRIGGER & FUNCTION:
DROP FUNCTION IF EXISTS $SCHEMA_REPORT.portfolio_return_insert_redirect_to_cache() CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_REPORT.portfolio_return_insert_redirect_to_cache()
	RETURNS trigger
	LANGUAGE 'plpgsql'
	COST 100
-- 	VOLATILE NOT LEAKPROOF
AS \$BODY\$
DECLARE
BEGIN
	IF (TG_OP = 'INSERT') THEN
		INSERT INTO $SCHEMA_DATA.silo_portfolio_return_cache(
			customer_id,
			portfolio_return,
			published_at,
			created_by_name,
			created_by_ip_address,
			status
		) 
		SELECT 
			NEW.customer_id, 
			NEW.portfolio_return, 
			NEW.date,
			current_user,
			inet_client_addr(),
			'new'
			; 
		RETURN Null;
	ELSE
		RETURN Null;
	END IF;
END;
\$BODY\$;




-- Trigger: before_modify

DROP TRIGGER IF EXISTS before_modify_001 ON $SCHEMA_REPORT.portfolio_return;
CREATE TRIGGER before_modify_001
	INSTEAD OF INSERT OR DELETE OR UPDATE 
	ON $SCHEMA_REPORT.portfolio_return
	FOR EACH ROW
	EXECUTE FUNCTION $SCHEMA_REPORT.portfolio_return_insert_redirect_to_cache();



-- =====================
-- take new record from FORM TABLE $SCHEMA_DATA.silo_asset_price_scalar_cache. Validate it. INSERT into $SCHEMA_DATA.silo_asset_price_scalar
-- =====================

-- DROP CASCADE: TRIGGER & FUNCTION:
DROP FUNCTION IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target() CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target()
	RETURNS trigger
	LANGUAGE 'plpgsql'
	COST 100
	VOLATILE NOT LEAKPROOF


-- DROP PROCEDURE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target();
-- CREATE OR REPLACE PROCEDURE $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target()
-- LANGUAGE 'plpgsql'

AS \$BODY\$
DECLARE
	_cache_id bigint; 
	_asset_name text;
	_asset_id bigint := NULL;
	_attribute_name text;
	_attribute_id bigint := NULL;
	_value numeric;
	_published_at_cet timestamp;
	_created_by_name text;
	_duplicate_id bigint := NULL;
	_record_inserted_id integer;
	_status text := 'processing';
	_msg text[];
BEGIN
	-- GET DATA FROM CASHED RECORD:
	SELECT 
		id, 
		asset, 
		attribute, 
		value, 
		published_at_cet,
		created_by_name
	INTO 
		_cache_id ,
		_asset_name,
		_attribute_name,
		_value,
		_published_at_cet,
		_created_by_name
	FROM $SCHEMA_DATA.silo_asset_price_scalar_cache
	WHERE
		1=1
		AND status = 'new'
	ORDER BY
		created_at_cet ASC
	LIMIT 1;
-- 	COMMIT;

	-- do nothing if no rows with status 'new': 
	IF _cache_id IS NULL THEN 
		RETURN NULL;
	END IF;

	RAISE NOTICE 'Processing cash row ID: %', _cache_id;
	UPDATE $SCHEMA_DATA.silo_asset_price_scalar_cache
	SET status = _status
	WHERE 
		id = _cache_id;
-- 	COMMIT;

	-- CONVERT ASSET AND ATTRIBUTE NAMES INTO IDs:
	_asset_id := (SELECT id FROM $SCHEMA_DATA.catalog_asset where name ilike coalesce(_asset_name, 'null') limit 1 );
	_attribute_id := (SELECT id FROM $SCHEMA_DATA.catalog_attribute where name ilike coalesce(_attribute_name, 'null') limit 1);

	-- REPORT ERROR IF ASSET WAS NOT FOUND IN CATALOG:
	IF _asset_id IS NULL
		THEN 
		_msg := array_append(_msg, 'wrong asset name: ' || _asset_name);
		_status := 'error';
	END IF;

	-- REPORT ERROR IF ATRIBUTE WAS NOT FOUND IN CATALOG:
	IF _attribute_id IS NULL
		THEN 
		_msg := array_append(_msg, 'wrong attribute name: ' || _attribute_name);
		_status := 'error';
	END IF;

	-- IF NO ERRORS, TRY TO FIND DUPLICATES:
	IF NOT _status = 'error' THEN
		_duplicate_id := (
			SELECT id
			FROM $SCHEMA_DATA.silo_asset_price_scalar
			WHERE
				1=1
				AND asset_id = _asset_id
				AND attribute_id = _attribute_id
				AND published_at_cet = _published_at_cet	
				AND value = _value
			ORDER BY 
				id DESC
			LIMIT 1
				)::bigint;

		-- REPORT ERROR IF DUPLICATE WAS FOUND:
		IF _duplicate_id IS NOT NULL
		   THEN 
		   _msg := array_append(_msg, 'id: ' || _duplicate_id);
		   _status := 'duplicate';
		END IF;
	END IF;
	
	-- IF EVERYTHIN IS FINE, TRY TO FIND PREVIOUS VERSIONS, MARK THEM, INSERT NEW ONE:
	IF NOT _status IN ('error', 'duplicate') THEN
		UPDATE $SCHEMA_DATA.silo_asset_price_scalar
		SET data_validation = data_validation + 1
		WHERE
			1=1
			AND asset_id = _asset_id
			AND attribute_id = _attribute_id
			AND published_at_cet = _published_at_cet
		;
-- 		COMMIT;

		INSERT INTO $SCHEMA_DATA.silo_asset_price_scalar (
			asset_id,
			attribute_id,
			value,
			published_at_cet,
			created_by
		) VALUES
		(
			_asset_id,
			_attribute_id,
			_value,
			_published_at_cet,
			_created_by_name
		)
		RETURNING id INTO _record_inserted_id;
-- 		COMMIT;

		-- REPORT SUCCESS WITH ID OF NEW RECORD:
		_status := 'done';
		_msg := array_append(_msg, _record_inserted_id::text);

	END IF;

	RAISE NOTICE 'Process is completed for cashed row ID: %', _cache_id;

	-- FINALLY UPDATE STATUS IN CACHED TABLE:
	UPDATE $SCHEMA_DATA.silo_asset_price_scalar_cache
	SET 
		status = _status,
		msg = array_to_string(_msg, '. ')
	WHERE 
		id = _cache_id
	;
-- 	COMMIT;
	
	-- Need to return something due to nature of function:
	RETURN Null;
END;
\$BODY\$;





DROP TRIGGER IF EXISTS after_modify_001 ON $SCHEMA_DATA.silo_asset_price_scalar_cache;
CREATE TRIGGER after_modify_001
	AFTER INSERT OR UPDATE 
	ON $SCHEMA_DATA.silo_asset_price_scalar_cache
	FOR EACH ROW
	EXECUTE FUNCTION $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target();




-- =====================
-- take new record from FORM TABLE $SCHEMA_DATA.silo_portfolio_return_cache. Validate it. INSERT into $SCHEMA_DATA.silo_portfolio_return
-- =====================

-- DROP CASCADE: TRIGGER & FUNCTION:
DROP FUNCTION IF EXISTS $SCHEMA_DATA.silo_portfolio_return_cache_to_target() CASCADE;
CREATE OR REPLACE FUNCTION $SCHEMA_DATA.silo_portfolio_return_cache_to_target()
	RETURNS trigger
	LANGUAGE 'plpgsql'
	COST 100
	VOLATILE NOT LEAKPROOF


-- DROP PROCEDURE IF EXISTS $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target();
-- CREATE OR REPLACE PROCEDURE $SCHEMA_DATA.silo_asset_price_scalar_cache_to_target()
-- LANGUAGE 'plpgsql'

AS \$BODY\$
DECLARE
	-- DECLARE VARIABLES:
	_cache_id bigint; 
	_customer_id bigint := NULL;
	_portfolio_return numeric;
	_published_at timestamp;
	_created_by_name text;
	_duplicate_id bigint := NULL;
	_record_inserted_id integer;
	_status text := 'processing';
	_msg text[];
BEGIN
	-- GET DATA FROM CASHED RECORD:
	SELECT 
		id, 
		customer_id,
		portfolio_return,
		published_at,
		created_by_name
	INTO 
		_cache_id ,
		_customer_id,
		_portfolio_return,
		_published_at,
		_created_by_name
	FROM $SCHEMA_DATA.silo_portfolio_return_cache
	WHERE
		1=1
		AND status = 'new'
	ORDER BY 
		created_at_cet ASC
	LIMIT 1;
-- 	COMMIT;

	-- do nothing if no rows with status 'new': 
	IF _cache_id IS NULL THEN 
		RETURN NULL;
	END IF;

	RAISE NOTICE 'Processing cash row ID: %', _cache_id;
	UPDATE $SCHEMA_DATA.silo_portfolio_return_cache
	SET status = _status
	WHERE 
		1 = 1
		AND id = _cache_id;
-- 	COMMIT;

	-- REPORT ERROR IF ASSET WAS NOT FOUND IN CATALOG:
	IF (SELECT 1 FROM $SCHEMA_DATA.catalog_customer WHERE id = coalesce(_customer_id, -1) ) IS NULL
		THEN 
		_msg := array_append(_msg, 'wrong customer id: ' || _customer_id);
		_status := 'error';
	END IF;

	-- IF NO ERRORS, TRY TO FIND DUPLICATES:
	IF NOT _status = 'error' THEN
		_duplicate_id := (
			SELECT id
			FROM $SCHEMA_DATA.silo_portfolio_return
			WHERE
				1=1
				AND customer_id = _customer_id
				AND published_at = _published_at	
				AND portfolio_return = _portfolio_return
			ORDER BY 
				id DESC
			LIMIT 1
				)::bigint;

		-- REPORT ERROR IF DUPLICATE WAS FOUND:
		IF _duplicate_id IS NOT NULL
		   THEN 
		   _msg := array_append(_msg, 'id: ' || _duplicate_id);
		   _status := 'duplicate';
		END IF;
	END IF;
	
	-- IF EVERYTHIN IS FINE, TRY TO FIND PREVIOUS VERSIONS, MARK THEM, INSERT NEW ONE:
	IF NOT _status IN ('error', 'duplicate') THEN
		UPDATE $SCHEMA_DATA.silo_portfolio_return
		SET data_validation = data_validation + 1
		WHERE
			1=1
			AND customer_id = _customer_id
			AND published_at = _published_at
		;
-- 		COMMIT;

		INSERT INTO $SCHEMA_DATA.silo_portfolio_return (
			customer_id,
			portfolio_return,
			published_at,
			created_by
		) VALUES
		(
			_customer_id,
			_portfolio_return,
			_published_at,
			_created_by_name
		)
		RETURNING id INTO _record_inserted_id;
-- 		COMMIT;

		-- REPORT SUCCESS WITH ID OF NEW RECORD:
		_status := 'done';
		_msg := array_append(_msg, _record_inserted_id::text);

	END IF;

	RAISE NOTICE 'Process is completed for cashed row ID: %', _cache_id;

	-- FINALLY UPDATE STATUS IN CACHED TABLE:
	UPDATE $SCHEMA_DATA.silo_portfolio_return_cache
	SET 
		status = _status,
		msg = array_to_string(_msg, '. ')
	WHERE 
		id = _cache_id
	;
-- 	COMMIT;
	
	-- Need to return something due to nature of function:
	RETURN Null;
END;
\$BODY\$;





DROP TRIGGER IF EXISTS after_modify_001 ON $SCHEMA_DATA.silo_portfolio_return_cache;
CREATE TRIGGER after_modify_001
	AFTER INSERT OR UPDATE 
	ON $SCHEMA_DATA.silo_portfolio_return_cache
	FOR EACH ROW
	EXECUTE FUNCTION $SCHEMA_DATA.silo_portfolio_return_cache_to_target();



EOSQL

# FIXME: without this sleep db will not finish import because entrypoint.sh of
#        official postgres docker image shutdowns server after custom entrypoints scripts
sleep 1