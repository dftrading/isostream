# isostream
ISOStream API

A simple wrapper around the ISOStream API that produces JSON or well formated pandas DataFrames from query results.
The the API client, methods, inputs and responses and all dynamically created from the OpenAPI specification of the IsoStream REST API.


## Installation

You can install the library with:
```
pip install isostream
```

## Quickstart
The ISOStream client is dynamically generated from the OpenAPI specification from the ISOStream API.
See the [ISOStream API Documentation](https://app.isostream.io/docs) for more details.
All of the REST API endpoints available on client as methods.

```
from isostream import IsoStream

client = IsoStream("<your_api_key>")

df = client.dalmp_node(node="WESTERN HUB", start="2021-01-01", end="2021-02-01", iso="PJM")
```

All string timestamps can also be provided as datetime objects.

To see a list of available API methods:
```
client.api_methods()

# or filter by a specific keyword to see only relevant methods:
client.api_methods(filter="dalmp")
```

By default, the client returns all queries in appropriated typed and logically pivoted pandas DataFrames.
You can alter this behavior with the 'as_df' and 'pivot' flags:
```
raw = client.meta_nodes(iso="PJM", as_df=False)
```


