# dbnomics-python-client -- Access DBnomics time series from Python
# By: Christophe Benz <christophe.benz@cepremap.org>
#
# Copyright (C) 2017 Cepremap
# https://git.nomics.world/dbnomics/dbnomics-python-client
#
# dbnomics-python-client is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# dbnomics-python-client is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


"""
This is the DBnomics Python client allowing to download time series from DBnomics Web API.

The `fetch_*` functions call DBnomics Web API as many times as necessary to download the wanted number of series.

The DBnomics Web API base URL can be customized by using the `api_base_url` parameter.
This will probably never be useful, unless somebody deploys a new instance of DBnomics under another domain name.
"""


import json
import pandas as pd
import urllib.parse

from .internals import api_version_matches, fetch_series_json_page


default_api_base_url = 'https://api.next.nomics.world'
default_max_nb_series = 50


class TooManySeries(Exception):
    def __init__(self, num_found, max_nb_series):
        message = (
            "DBnomics Web API found {num_found} series matching your request, " +
            (
                "but you passed the argument 'max_nb_series={max_nb_series}'."
                if max_nb_series is not None
                else "but you did not pass any value for the 'max_nb_series' argument, "
                     "so a default value of {default_max_nb_series} was used."
            ) +
            " Please give a higher value (at least max_nb_series={num_found}), and try again."
        ).format(
            default_max_nb_series=default_max_nb_series,
            max_nb_series=max_nb_series,
            num_found=num_found,
        )
        super().__init__(message)


def fetch_series(provider_code, dataset_code, series_code=None, max_nb_series=None, api_base_url=None):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API.

    If `series_code` is given as a string, return this series.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Examples:
    - fetch_series("AMECO", "ZUTN")
    - fetch_series("AMECO", "ZUTN", "EA19.1.0.0.0.ZUTN")
    """
    # Parameters validation
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url is None:
        api_base_url = default_api_base_url
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/series?provider_code={}&dataset_code={}'.format(provider_code, dataset_code) \
        if series_code is None \
        else api_base_url + '/series?series_ids={}/{}/{}'.format(provider_code, dataset_code, series_code)
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series)


def fetch_series_by_ids(series_ids, max_nb_series=None, api_base_url=None):
    """Download time series from DBnomics Web API, given a list of series IDs.

    The `series_ids` parameter must be a non-empty `list` of series IDs.
    A series ID is a tuple like `(provider_code, dataset_code, series_code)`.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Example: fetch_series_by_ids([("AMECO", "ZUTN", "EA19.1.0.0.0.ZUTN"), ("AMECO", "ZUTN", "EU28.1.0.0.0.ZUTN")])
    """
    # Parameters validation
    assert isinstance(series_ids, list) and series_ids, series_ids
    assert all(isinstance(series_id, (tuple, list)) and len(series_id) == 3 for series_id in series_ids), series_ids
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url is None:
        api_base_url = default_api_base_url
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_ids_str = ",".join(
        map(lambda series_id: "/".join(series_id), series_ids)
    )
    series_json_url = api_base_url + '/series?series_ids={}'.format(series_ids_str)
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series)


def fetch_series_by_dimensions(provider_code, dataset_code, dimensions, max_nb_series=None,
                               api_base_url=None):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    searching by dimensions.

    The `dimensions` parameter must be a non-empty `dict` of dimensions (non-empty `list` of non-empty `str`), like so:
    `{"freq": ["A", "M"], "country": ["FR"]}`.
    Instead of passing an empty `dict`, please use the `fetch_series` function.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Example: `fetch_series_by_dimensions("AMECO", "ZUTN", {"geo": ["fra"]})`
    """
    # Parameters validation
    assert isinstance(dimensions, dict), dimensions
    assert dimensions, dimensions
    assert all(isinstance(s, str) and s for s in dimensions.keys()), dimensions
    assert all(
        isinstance(l, list) and l and all(isinstance(s, str) and s for s in l)
        for l in dimensions.values()
    ), dimensions
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url is None:
        api_base_url = default_api_base_url
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/series?provider_code={}&dataset_code={}&dimensions={}'.format(
        provider_code, dataset_code, json.dumps(dimensions))
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series)


def fetch_series_by_sdmx_filter(provider_code, dataset_code, sdmx_filter, max_nb_series=None, api_base_url=None):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    given a SDMX filter.

    Some providers are not compatible with SDMX filters.
    To be compatible, providers series codes must be composed of dimensions values codes, separated by a '.',
    like `M.QA.PCPIEC_WT`.
    For example, "IMF", "Eurostat" and "INSEE" are compatible providers, but "Bank of England" is not.

    A SDMX filter can designate many series, whereas a series code designates one series. It allows to:
    - remove a constraint on a dimension, for example `M..PCPIEC_WT`;
    - enumerate many values for a dimension, separated by a '+', for example `M.FR+DE.PCPIEC_WT`;
    - combine these possibilities many times in the same SDMX filter.

    If the original series code is passed to the `sdmx_filter` parameter, this function will behave like `fetch_series`,
    because the series code can be considered as a SDMX filter which all dimensions are constrained.

    Also, if the rightmost dimension value code is removed, then the final '.' can be removed too: `A.FR.` = `A.FR`.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Examples:
    - fetch_series_by_sdmx_filter("IMF", "CPI", "M.QA.PCPIEC_WT")
    - fetch_series_by_sdmx_filter("IMF", "CPI", "M.FR+DE.PCPIEC_WT")
    - fetch_series_by_sdmx_filter("IMF", "CPI", ".FR.PCPIEC_WT")
    - fetch_series_by_sdmx_filter("IMF", "CPI", "M..PCPIEC_IX+PCPIA_IX")
    """
    # Parameters validation
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url is None:
        api_base_url = default_api_base_url
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/series?provider_code={}&dataset_code={}&sdmx_filter={}' \
        .format(provider_code, dataset_code, urllib.parse.quote(sdmx_filter))
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series)


def fetch_series_by_url(url, max_nb_series=None):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    giving the URL of the series as found on the website (search for "API link" links).

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Example: `fetch_series_by_url("https://api.next.nomics.world/series?provider_code=AMECO&dataset_code=ZUTN")`
    """
    # Parameters validation
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series

    series_list = []
    offset = 0
    dataset_code = UnboundLocalError
    dataset_name = UnboundLocalError

    while True:
        response_json = fetch_series_json_page(url, offset=offset)

        dataset_json = response_json["dataset"]

        response_dataset_code = dataset_json["code"]
        if dataset_code is UnboundLocalError:
            dataset_code = response_dataset_code
        else:
            assert dataset_code == response_dataset_code, (dataset_code, response_dataset_code)

        response_dataset_name = dataset_json.get("name")
        if dataset_name is UnboundLocalError:
            dataset_name = response_dataset_name
        else:
            assert dataset_name == response_dataset_name, (dataset_name, response_dataset_name)

        series_json_page = response_json["series"]
        num_found = series_json_page['num_found']
        if max_nb_series is None and num_found > default_max_nb_series:
            raise TooManySeries(num_found, max_nb_series)

        series_list.extend(series_json_page['data'])
        nb_series = len(series_list)

        # Stop if we have enough series.
        if max_nb_series is not None and nb_series >= max_nb_series:
            # Do not respond more series than the asked max_nb_series.
            if nb_series > max_nb_series:
                series_list = series_list[:max_nb_series]
            break

        # Stop if we downloaded all the series.
        assert nb_series <= num_found, (nb_series, num_found)  # Can't download more series than num_found.
        if nb_series == num_found:
            break

        offset += nb_series

    dataframe = pd.concat(map(pd.DataFrame, series_list))
    dataframe["dataset_code"] = dataset_code
    dataframe["dataset_name"] = dataset_name
    return dataframe
