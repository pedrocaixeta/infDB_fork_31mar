# InfDB Python Wrapper

This python package helps to work with the infDB and provides basic functions such as
- connection to Postgres
- loading configuration
- central and preformated logging

# Get started
```bash
uv pip install infdb
```

```python
import infdb as InfDB

# Load InfDB handler
infdb = InfDB(tool_name="preprocessor")

# Database connection
infdbclient_citydb = infdb.connect(db_name="citydb")

# Logger setup
infdblog = infdb.get_log()
```