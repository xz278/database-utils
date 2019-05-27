# Database utils
## Usage
```python
from database_utils import database_utils

database_utils.list_connection()
database_utils.inspect_connection('redshift_conf')
client = database_utils.client(name='redshift_conf')
client.read(query='select max(update_date) from table')
```