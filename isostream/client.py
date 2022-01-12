from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List, Union

import pandas as pd
import requests
from dateutil.parser import parse

from .utils import ApiException, time_chunk


class IsoStream:
    """
    IsoStream base client

    Parameters
    ----------
    api_key : str
        Your ISOStream API key
    verbose : bool, default = False
        Print verbose information
    """

    _host = "https://app.isostream.io/api"
    _format = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, api_key: str, verbose: bool = False) -> None:
        """Initialize the client"""
        self._api_key = api_key
        self._verbose = verbose
        self._session = requests.Session()
        self._api_spec = self._session.get(self._host + "/openapi.json").json()
        self._paths = {}

        for path, info in self._api_spec["paths"].items():
            method = "get" if "get" in info else "post"
            self._paths[path] = info[method]
            self._paths[path]["_method"] = method

        self._create_methods()

    def _make_docstring(self, path: str) -> str:
        """Return a custom docstring for a method based on the OpenAPI path specification"""
        strs = []
        for arg_def in self._paths[path]["parameters"]:
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
            strs.append(f"\n{name} : {_type}, required = {req} \n    {desc}")
        docstr = "".join(strs)
        return (
            f"Wrapper method for API call to {path} \n\n"
            "Parameters \n"
            "----------"
            f"{docstr} \n"
            "as_df : bool, default = True \n"
            "    Return the result as a pandas DataFrame, or as raw result \n"
            "pivot : bool, default = False \n"
            "    If returning a DataFrame, whether to pivot it to a more useful format \n"
            "chunk : int, default = 365 \n"
            "    If the query is a timeseries query, break the query into chunk day intervals \n\n"
            "Returns\n"
            "-------\n"
            "List[Dict] or pd.DataFrame \n\n"
        )

    def _create_methods(self) -> None:
        """Create all the methods from the OpenAPI Spec and attach them to the class as partial functions"""
        for path in self._api_spec["paths"]:

            def member_func(path, as_df: bool = True, pivot: bool = False, **kwargs):
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
        method = self._paths[path]["_method"]
        resp = self._session.request(method, self._host + path, params=params)
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
        return resp.json()

    def _path_to_name(self, path: str) -> str:
        """Return the method name for a given path

        Parameters
        ----------
        path : str
            The URL path to query

        Returns
        -------
        str : The method name
        """
        return path.replace("/", "_").strip("_")

    def _format_df(
        self,
        path: str,
        resp: List[Dict],
        guess_pivot: bool = True,
    ) -> pd.DataFrame:
        """Return a properly formatting dataframe

        Parameters
        ----------
        path : str
            The URL path to query
        resp : List[Dict]
            The response from the API
        guess_pivot : bool, default = True
            If the query is a timeseries query, break the query into chunk day intervals

        Returns
        -------
        pd.DataFrame : The formatted dataframe
        """
        df = pd.DataFrame(resp)
        resp_type = self._paths[path]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["items"]["$ref"].split("/")[-1]
        schema = self._api_spec["components"]["schemas"][resp_type]
        for name, info in schema["properties"].items():
            if "type" not in info:
                continue
            if name not in df.columns:
                continue
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

    def _is_time_query(self, kwargs: dict) -> bool:
        """Return whether the query is a time series query"""
        return "start" in kwargs and "end" in kwargs

    def _api_get(
        self,
        path: str,
        as_df: bool = True,
        pivot: bool = True,
        chunk: int = 365,
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

        Returns
        -------
        Union[pd.DataFrame, List[Dict]] : The result of the API call
        """
        params = {}
        m = self._path_to_name(path)
        for arg in self._paths[path]["parameters"]:
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

        if self._is_time_query(params):
            resp = []
            for _start, _end in time_chunk(
                parse(params["start"]),
                parse(params["end"]),
                delta=timedelta(days=chunk),
            ):
                params["start"] = _start
                params["end"] = _end
                resp += self._get(path, params)
        else:
            resp = self._get(path, params)

        if as_df:
            return self._format_df(path, resp, guess_pivot=pivot)
        return resp

    def api_methods(self, filter: str = None) -> None:
        """Print all available API Methods. Optionally filter methods on keyword

        Parameters
        ----------
        filter : str, default = None
            The keyword to filter methods on

        Returns
        -------
        None
        """
        for path in self._api_spec["paths"]:
            if filter and filter not in path:
                continue
            print(f"Method {self._path_to_name(path)}:")
            print(self._make_docstring(path).replace("\n", "\n\t"))
