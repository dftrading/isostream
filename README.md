# isostream
ISOStream API

A simple wrapper around the ISOStream API

## Installation

You can install the library with:
```
    pip install py-isostream
```

## Quickstart

```
    from isostream import IsoStream

    client = IsoStream("<your_api_key_>")

    df = client.dalmp(["1",], "2020-01-01", "2021-01-01")
```