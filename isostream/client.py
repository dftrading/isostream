from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, Generator, List, Union

import pandas as pd
import requests
import requests_cache

HOST = "https://app.isostream.io/api"


class IsoStream:
    """
    IsoStream base client

    Parameters
    ----------
    api_key : str
        Your ISOStream API key
    use_cache : bool, default = True
        Whether or not to use cached data where possible
    """

    _format = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, api_key: str, use_cache: bool = False):
        self._api_key = api_key
        self._use_cache = use_cache
        if use_cache:
            self._session = requests_cache.CachedSession("isostream")
        else:
            self._session = requests.Session()
        self._api_spec = self._session.get(HOST + "/openapi.json").json()
        self._create_methods()

    def _make_docstring(self, path: str) -> str:
        """Return a custom docstring based on the path specification"""
        docstr = ""
        for arg_def in self._api_spec["paths"][path]["get"]["parameters"]:
            name = arg_def["name"]
            if name == "api_key":
                continue
            req = arg_def["required"]
            if "$ref" in arg_def["schema"]:
                key = arg_def["schema"]["$ref"].split("/")[-1]
                comp = self._api_spec["components"]["schemas"][key]
                desc = comp["description"]
                _type = comp["type"] + ", " + ",".join(comp.get("enum", []))
            else:
                _type = arg_def["schema"].get("type")
                desc = arg_def.get("description")
            docstr += f"{name} : {_type}, required = {req} \n    {desc}\n\n"
        return docstr

    def _create_methods(self) -> None:
        for path in self._api_spec["paths"]:

            def member_func(path, **kwargs):
                return self._api_get(path, **kwargs)

            method_name = path.replace("/", "_").strip("_")
            method = partial(member_func, path)
            method.__name__ = method_name
            docstr = self._make_docstring(path)
            method.__doc__ = f"Wrapper method for API call to {path}\n\nParameters\n----------\n{docstr}\nReturns\n-------\nList[Dict]"
            setattr(self, method_name, method)

    def _get(self, path: str, params: Dict) -> List[Dict]:
        """GET a path with parameters

        Parameters
        ----------
        path : str
            The URL path to query
        params : dict
            A dictionary of query parameters

        Returns
        -------
        requests.Response
        """
        params["api_key"] = self._api_key
        print(HOST + path, params)
        resp = self._session.get(HOST + path, params=params)
        if resp.status_code != 200:
            raise Exception(f"Error in API Call: {resp.text}")

        return resp.json()

    def _range(self, start: datetime, end: datetime, delta: timedelta) -> Generator:
        """Return a generator that produces timestamp splits that area delta time apart"""
        _start = start
        while _start < end:
            _end = min(_start + delta, end)
            yield _start, _end
            _start = _end

    def _api_get(
        self,
        path: str,
        as_df: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, List[Dict]]:
        params = {}
        for arg in self._api_spec["paths"][path]["get"]["parameters"]:
            name = arg["name"]
            if name == "api_key":
                continue
            if arg["schema"].get("format") == "date-time":
                params[name] = kwargs.pop(name).strftime(self._format)
            else:
                if arg["required"]:
                    params[name] = kwargs.pop(name)
                else:
                    params[name] = kwargs.pop(name, None)

        if kwargs:
            invalid = ",".join(list(kwargs.keys()))
            raise TypeError(f"Unknown input parameters: {invalid}")
        resp = self._get(path, params)

        if as_df:
            return pd.DataFrame(resp)
        return resp
