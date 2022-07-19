#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL

CREATE GROUP developers;
CREATE GROUP readers;
CREATE GROUP $PROJECT_USERGROUP;

CREATE USER $DB_USER_NAME WITH 
LOGIN
NOSUPERUSER
INHERIT
NOCREATEDB
NOCREATEROLE
NOREPLICATION
PASSWORD '$DB_USER_PASSWORD';

GRANT $PROJECT_USERGROUP TO $DB_USER_NAME;



drop database if exists $PROJECT_DATABASE;
create database $PROJECT_DATABASE
	WITH 
	OWNER = postgres
	ENCODING = 'UTF8'
	LC_COLLATE = 'en_US.utf8'
	LC_CTYPE = 'en_US.utf8'
	TABLESPACE = pg_default
	CONNECTION LIMIT = -1;

EOSQL

# FIXME: without this sleep db will not finish import because entrypoint.sh of
#        official postgres docker image shutdowns server after custom entrypoints scripts
sleep 1