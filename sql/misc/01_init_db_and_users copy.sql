-- psql  postgres -f /path/to_this_script/this_script.sql

-- ===========================================================
--   CREATE DATABASE
-- ===========================================================
-- SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dwarehouse';
drop database if exists dwarehouse;
create database dwarehouse;

-- ===========================================================
--   CREATE GROUPS
-- ===========================================================

DROP ROLE IF EXISTS developers;
CREATE GROUP developers;
DROP ROLE IF EXISTS readers;
CREATE GROUP readers;
DROP ROLE IF EXISTS writers;
CREATE GROUP writers;

-- now switch to new db:
-- \c dwarehouse