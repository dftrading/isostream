from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, Generator, List, Type, Union

import pandas as pd
import requests
import requests_cache
from dateutil.parser import parse

from .utils import ApiException

HOST = "https://app.isostream.io/api"


class IsoStream:
    """
    IsoStream base client

    Parameters
    ----------
    api_key : str
        Your ISOStream API key
    verbose : bool, default = False
        Print verbose information
    use_cache : bool, default = True
        Whether or not to use cached data where possible.
    cache_name : str, default = isostream_cache"
        The name of the cache
    cache_backend : str, default='sqlite'
        The cache backed to use.  Can be 'sqlite', 'filesystem', 'mongodb', 'gridfs', 'redis', 'dynamodb', 'memory'.
    """

    _format = "%Y-%m-%dT%H:%M:%S"

    def __init__(
        self,
        api_key: str,
        verbose: bool = False,
        use_cache: bool = True,
        cache_name: str = "isostream_cache",
        cache_backend: str = "sqlite",
        host: str = HOST,
    ):
        self._api_key = api_key
        self._use_cache = use_cache
        self._verbose = verbose
        self._host = host
        if use_cache:
            self._session = requests_cache.CachedSession(
                cache_name, backend=cache_backend
            )
        else:
            self._session = requests.Session()
        self._api_spec = self._session.get(self._host + "/openapi.json").json()
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
                _type = comp["type"] + ", " + ",".join(comp.get("enum", []))
                desc = comp["description"]
            else:
                _type = arg_def["schema"].get("type")
                desc = arg_def.get("description")
            docstr += f"\n{name} : {_type}, required = {req} \n    {desc}"

        return (
            f"Wrapper method for API call to {path} \n\n"
            "Parameters \n"
            "----------"
            f"{docstr} \n"
            "as_df : bool, default = True \n"
            "    Return the result as a pandas DataFrame, or as raw result \n"
            "pivot : bool, default = True \n"
            "    If returning a DataFrame, whether to pivot it to a more useful format \n\n"
            "Returns\n"
            "-------\n"
            "List[Dict] or pd.DataFrame\n\n"
        )

    def _create_methods(self) -> None:
        """Create all the methods from the OpenAPI Spec and attach them to the class"""
        for path in self._api_spec["paths"]:

            def member_func(path, as_df: bool = True, pivot: bool = True, **kwargs):
                return self._api_get(path, as_df=as_df, pivot=pivot, **kwargs)

            method = partial(member_func, path)
            method.__name__ = self._path_to_name(path)
            method.__doc__ = self._make_docstring(path)
            setattr(self, method.__name__, method)

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
        List[Dict]
        """
        params["api_key"] = self._api_key
        if self._verbose:
            print(self._host + path, params)
        resp = self._session.get(self._host + path, params=params)
        if resp.status_code != 200:
            try:
                msg = ",".join(
                    [
                        "parameter '" + x["loc"][1] + "': " + x["msg"]
                        for x in resp.json()["detail"]
                    ]
                )
            except Exception:
                msg = resp.text
            raise ApiException(f"Error in API Call: {msg}")
        json = resp.json()
        if not json:
            raise ApiException(f"Query parameters produced no results: {params}")

        return json

    def _range(self, start: datetime, end: datetime, delta: timedelta) -> Generator:
        """Return a generator that produces timestamp splits that area delta time apart"""
        _start = start
        while _start < end:
            _end = min(_start + delta, end)
            yield _start, _end
            _start = _end

    def _path_to_name(self, path: str) -> str:
        """Return the method name for a given path"""
        return path.replace("/", "_").strip("_")

    def _format_df(self, path: str, resp: List, guess_pivot=True) -> pd.DataFrame:
        """Return a properly formatting dataframe"""
        df = pd.DataFrame(resp)
        resp_type = self._api_spec["paths"][path]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["items"]["$ref"].split("/")[-1]
        schema = self._api_spec["components"]["schemas"][resp_type]
        for name, info in schema["properties"].items():
            if info["type"] == "number":
                df[name] = df[name].astype("float64")
            elif info["type"] == "string":
                if info.get("format") == "date-time":
                    df[name] = df[name].astype("datetime64")
                else:
                    df[name] = df[name].astype("string")

        if guess_pivot:
            idx = df.dtypes.index[df.dtypes == "datetime64[ns]"]
            cols = df.dtypes.index[df.dtypes == "string"]
            if not idx.empty and not cols.empty:
                return df.pivot(index=idx[0], columns=cols[0])
            elif not cols.empty:
                return df.set_index(cols[0])
        return df

    def _api_get(
        self,
        path: str,
        as_df: bool = True,
        pivot: bool = True,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, List[Dict]]:
        """A generic api call

        Parameters
        ----------
        path : str
            The path of the API to call
        as_df : boo, default = True
            Whether to return result as a dataframe or not
        pivot : bool, default = True
            If returning a DataFrame, pivot the resulting dataframe
        **kwargs : Any
            The parameters for the method call
        """
        params = {}
        m = self._path_to_name(path)
        for arg in self._api_spec["paths"][path]["get"]["parameters"]:
            name = arg["name"]
            if name == "api_key":
                continue
            if arg["required"]:
                try:
                    p = kwargs.pop(name)
                except KeyError:
                    raise TypeError(f"{m}() missing keyword-only argument '{name}'")
            else:
                p = kwargs.pop(name, None)
            if p and arg["schema"].get("format") == "date-time":
                if isinstance(p, datetime):
                    p = p.strftime(self._format)
                else:
                    p = parse(p).strftime(self._format)
            params[name] = p

        if kwargs:
            invalid = ",".join(list(kwargs.keys()))
            raise TypeError(f"{m}() got an unexpected keyword argument: '{invalid}'")
        resp = self._get(path, params)

        if as_df:
            return self._format_df(path, resp, guess_pivot=pivot)
        return resp

    def api_methods(self, filter: str = None) -> None:
        """ "Print all available API Methods"""
        for path in self._api_spec["paths"]:
            if filter and filter not in path:
                continue
            print(f"Method {self._path_to_name(path)}:")
            print(self._make_docstring(path).replace("\n", "\n\t"))
