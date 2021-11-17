# isostream
ISOStream API

A simple wrapper around the ISOStream API that produces well formated pandas DataFrames from query results.

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

client = IsoStream("<your_api_key_>")

df = client.lmp_dalmp_node(node="1", start="2021-01-01", end="2021-02-01", iso="pjm")
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
raw = client.nodes_info(iso="pjm", as_df=False)
```

isostream uses the requests_cache library by default to cache any requests in a .sqlite file in your current directory.
You can disable this behavior with the 'use_cache' flag:
```
client = IsoStream("<api_key>", use_cache=False)

# Use the cache, but use in-memory cache only, so nothing is persisted between sessions:
client = IsoStream("<api_key>", cache_backend="memory")
```

