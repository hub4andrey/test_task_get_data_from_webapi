#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --dbname postgres <<-EOSQL
-- psql  postgres -f /path/to_this_script/this_script.sql


-- ===========================================================
--   CREATE GROUPS
-- ===========================================================

DROP ROLE IF EXISTS $DB_GROUP_R;
CREATE GROUP $DB_GROUP_R;
DROP ROLE IF EXISTS $DB_GROUP_W;
CREATE GROUP $DB_GROUP_W;

CREATE USER $DB_GEN_USER_NAME WITH 
LOGIN
NOSUPERUSER
INHERIT
NOCREATEDB
NOCREATEROLE
NOREPLICATION
PASSWORD '$DB_GEN_USER_PASSWORD';

GRANT $DB_GROUP_W TO $DB_GEN_USER_NAME;

-- ===========================================================
--   CREATE DATABASE
-- ===========================================================
-- SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dwarehouse';
drop database if exists $DB_NAME;
create database $DB_NAME
	WITH 
	OWNER = postgres
	ENCODING = 'UTF8'
	LC_COLLATE = 'en_US.utf8'
	LC_CTYPE = 'en_US.utf8'
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1;

GRANT CONNECT ON DATABASE $DB_NAME TO $DB_GROUP_W, $DB_GROUP_R;


EOSQL

# FIXME: without this sleep db will not finish import because entrypoint.sh of
#        official postgres docker image shutdowns server after custom entrypoints scripts
sleep 1