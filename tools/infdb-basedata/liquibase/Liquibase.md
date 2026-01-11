## Overview
- Liquibase is a database migration tool that helps manage and version control database schema.

- New database schema should be placed in ./infdb-versioning/mnt/liquibase/changelogs
and included into the changelog-master.yml file
-  every changelog must start with
```
--liquibase formatted sql
--changeset author:id
```
- the pair `(author,id)` must be unique
- Note that versioning is in general non-linear when using `includeAll` and the execution order can be set by the order of `include` in the changelog-master.yml file.


### Additional notes
- When using statement with $$, you need to specify it as raw SQL statement in the changeset and manual specify rollback operation. 
  - See create_buildings_function.sql for example
  - See https://support.liquibase.com/hc/en-us/articles/29383025767323--Error-Unterminated-dollar-quote for more information
- 