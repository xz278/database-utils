# Database utils
## Usage
```python
from database_utils import database_utils

database_utils.list_connection()
database_utils.inspect_connection('redshift_conf')
client = database_utils.client(name='redshift_conf')

# read query
client.read(query='select max(update_date) from table')

# write query
client.execute(query='drop table income')

# list tables in a database
client.list_tables()

# list columns in a table
client.list_columns(
	table='income',
	schema='public' # only required for postgres ro redshift
)

```