--liquibase formatted sql
--changeset marvin.huang:1.0.6.0
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM pg_extension WHERE extname = 'pgrouting'
SET SEARCH_PATH = public;
CREATE EXTENSION pgrouting CASCADE;
SET SEARCH_PATH = ${output_schema}, public;