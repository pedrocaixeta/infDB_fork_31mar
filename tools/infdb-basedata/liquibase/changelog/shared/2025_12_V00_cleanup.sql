--liquibase formatted sql
--changeset marvin.huang:1.0.0.0 labels:infdb-basedata,infdb-basedata-basedata
DROP SCHEMA IF EXISTS ${output_schema} CASCADE;

--changeset marvin.huang:1.0.0.1 labels:infdb-basedata,infdb-basedata-buildings
--comment: allows to clean up only building related table
DROP TABLE IF EXISTS ${output_schema}.buildings;
DROP TABLE IF EXISTS ${output_schema}.buildings_grid_100m;
DROP TABLE IF EXISTS ${output_schema}.buildings_grid_1km;
DROP TABLE IF EXISTS ${output_schema}.bld2ts;
DROP TABLE IF EXISTS ${output_schema}.bl2grid;

--changeset marvin.huang:1.0.0.9 labels:infdb-basedata,infdb-basedata-basedata

CREATE SCHEMA ${output_schema};