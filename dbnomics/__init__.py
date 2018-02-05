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

from .internals import api_version_matches, fetch_series_json_page


default_api_base_url = 'https://api.next.nomics.world'
default_max_nb_series = 50


class TooManySeries(Exception):
    def __init__(self, num_found, max_nb_series):
        message = (
            "DBnomics Web API found {num_found} series matching your request, " +
            (
                "but you gave the argument 'max_nb_series={max_nb_series}'."
                if max_nb_series is not None
                else "but you did not give the argument 'max_nb_series', so a default value of {default_max_nb_series} was used."
            ) +
            " Please give a higher value (at least {num_found}), and try again."
        ).format(
            default_max_nb_series=default_max_nb_series,
            max_nb_series=max_nb_series,
            num_found=num_found,
        )
        super().__init__(message)


def fetch_series(provider_code, dataset_code, max_nb_series=None, api_base_url=default_api_base_url):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.
    """
    # Parameters validation
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/{}/{}'.format(provider_code, dataset_code)
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series, api_base_url=api_base_url)


def fetch_series_by_codes(provider_code, dataset_code, series_codes, max_nb_series=None,
                          api_base_url=default_api_base_url):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    searching by series codes.

    The `series_codes` parameter must be a non-empty `list` of series codes (non-empty `str`), like so:
    `["EA19.1.0.0.0.ZUTN", "EU28.1.0.0.0.ZUTN"]`.
    Instead of passing an empty `list`, please use the `fetch_series` function.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.
    """
    # Parameters validation
    assert isinstance(series_codes, list), series_codes
    assert series_codes, series_codes
    assert all(isinstance(s, str) and s for s in series_codes), series_codes
    assert max_nb_series is None or max_nb_series >= 1, max_nb_series
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/{}/{}?series_codes={}'.format(
        provider_code, dataset_code, ",".join(series_codes))
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series, api_base_url=api_base_url)


def fetch_series_by_dimensions(provider_code, dataset_code, dimensions, max_nb_series=None,
                               api_base_url=default_api_base_url):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    searching by dimensions.

    The `dimensions` parameter must be a non-empty `dict` of dimensions (non-empty `list` of non-empty `str`), like so:
    `{"freq": ["A", "M"], "country": ["FR"]}`.
    Instead of passing an empty `dict`, please use the `fetch_series` function.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.
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
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]

    series_json_url = api_base_url + '/{}/{}?dimensions={}'.format(
        provider_code, dataset_code, json.dumps(dimensions))
    return fetch_series_by_url(series_json_url, max_nb_series=max_nb_series, api_base_url=api_base_url)


def fetch_series_by_url(url, max_nb_series=None, api_base_url=default_api_base_url):
    """Download time series of a particular dataset in a particular provider, from DBnomics Web API,
    giving the URL of the series as found on the website (search for "API link" links).

    Example: `fetch_series_by_url("https://api.next.nomics.world/Eurostat/ei_bsin_q_r2")`

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.
    """
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
