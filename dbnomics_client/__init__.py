#! /usr/bin/env python3


# dbnomics-python-client -- Access DB.nomics time series from Python
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

import itertools
import logging
from urllib.parse import parse_qs, quote, urlsplit, urlunsplit

import pandas as pd
import requests
import semver

api_min_version = '0.10.0'
api_max_version = '0.11.0'
default_api_base_url = 'https://api.next.nomics.world'
default_limit = 3000

log = logging.getLogger(__name__)


def api_version_matches(api_version):
    return semver.match(api_version, ">=" + api_min_version) and \
        semver.match(api_version, "<" + api_max_version)


def fetch(provider_code, dataset_code, series_codes=[], api_base_url=default_api_base_url, limit=default_limit):
    """Download time series from DB.nomics Web API, giving individual parameters.

    Return a Python Pandas `DataFrame`. A dataframe contains many time series.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.

    By default this function sets a limit of 3000 series, via the `dbnomics_client.default_limit` constant.
    Pass `limit=None` explicitly to download an unlimited number of series.

    The DB.nomics Web API base URL can be customized by passing a value to the `api_base_url` parameter.
    This will probably never be useful, unless somebody deploys a new instance of DB.nomics under another domain name.
    """
    assert isinstance(series_codes, list), series_codes
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    series_codes_param = "&".join(
        "series_code={}".format(series_code)
        for series_code in series_codes
    )
    series_json_url = api_base_url + '/{}/{}?{}'.format(provider_code, dataset_code, series_codes_param)
    return fetch_from_url(series_json_url, is_api_url=True, limit=limit)


def fetch_from_url(url, api_base_url=default_api_base_url, is_api_url=False, limit=default_limit):
    """Download time series from DB.nomics Web API, giving the URL of the series on DB.nomics website.

    Example: fetch_from_url("https://next.nomics.world/Eurostat/ei_bsin_q_r2")

    Return a Python Pandas `DataFrame`. A dataframe contains many time series.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.

    By default this function sets a limit of 3000 series, via the `dbnomics_client.default_limit` constant.
    Pass `limit=None` explicitly to download an unlimited number of series.

    If the `url` parameter is an URL of DB.nomics Web API (and not an URL of the website), the parameter `is_api_url`
    must be set to `True`. By default it is `False`.
    """
    def get_nb_series():
        return len(set(map(lambda e: e['code'], series_json_list)))
    if not is_api_url:
        url = website_url_to_api_url(url, api_base_url=api_base_url)
    series_json_list = []
    while True:
        dataset_json, series_json_page = fetch_series_json_page(url, offset=get_nb_series())
        dataset_code = dataset_json["code"]
        dataset_name = dataset_json.get("name")
        series_json_list.extend(list(iter_column_json_dicts(series_json_page['data'], dataset_code, dataset_name)))
        nb_series = get_nb_series()
        if (limit is not None and nb_series >= limit) or nb_series == series_json_page['num_found']:
            break
    return pd.DataFrame(series_json_list)


def fetch_series_json_page(series_json_url, offset):
    page_url = '{}{}offset={}'.format(
        series_json_url,
        '&' if '?' in series_json_url else '?',
        offset,
    )
    log.debug(page_url)
    response = requests.get(page_url)
    try:
        response_json = response.json()
    except ValueError as exc:
        raise ValueError('Could not parse JSON payload of response because: {}. Response text: {}'.format(
            exc, response.text))
    if response.status_code == 404:
        raise ValueError("Could not fetch data from URL {} because: {}".format(page_url, response_json['message']))
    api_version = response_json['_meta']['python_project_version']
    if not api_version_matches(api_version):
        raise ValueError('Web API version is {!r}, but this version of the Python client is expecting >= {}, < {}'.format(
            api_version, api_min_version, api_max_version))

    series_json_page = response_json.get('series')
    if series_json_page is None:
        raise ValueError('Could not find "series" key in response JSON payload. URL = {!r}, received: {!r}'.format(
            page_url, response_json))
    assert series_json_page['offset'] == offset, (series_json_page['offset'], offset)
    return response_json['dataset'], series_json_page


def iter_column_json_dicts(seq, dataset_code, dataset_name):
    """Transform the `list` of `dict` received by DB.nomics Web API to a `list` of `dict` compatible with Python Pandas,
    in order to build a `DataFrame`.

    >>> list(iter_column_json_dicts([
    ...     {
    ...         "code": "s1",
    ...         "FREQ": "M",
    ...         "period": ["2010", "2011", "2012"],
    ...         "value": [1, 2, 3],
    ...     },
    ...     {
    ...         "code": "s2",
    ...         "FREQ": "Q",
    ...         "period": ["2010"],
    ...         "value": [999],
    ...     }
    ... ], "ABCD", "Very cool dataset"))
    [{'code': 's1', 'FREQ': 'M', 'period': '2010', 'value': 1, 'dataset_code': 'ABCD', 'dataset_name': 'Very cool dataset'}, {'code': 's1', 'FREQ': 'M', 'period': '2011', 'value': 2, 'dataset_code': 'ABCD', 'dataset_name': 'Very cool dataset'}, {'code': 's1', 'FREQ': 'M', 'period': '2012', 'value': 3, 'dataset_code': 'ABCD', 'dataset_name': 'Very cool dataset'}, {'code': 's2', 'FREQ': 'Q', 'period': '2010', 'value': 999, 'dataset_code': 'ABCD', 'dataset_name': 'Very cool dataset'}]
    """
    def iter_expanded_dicts(d):
        assert "period" in d, d
        assert "value" in d, d
        assert len(d["period"]) == len(d["value"]), d
        keys_with_list = [
            k
            for k, v in d.items()
            if isinstance(v, list)
        ]
        for keys_with_list_values in zip(*(d[k] for k in keys_with_list)):
            column_json = d.copy()
            column_json["dataset_code"] = dataset_code
            if dataset_name:
                column_json["dataset_name"] = dataset_name
            for idx, k in enumerate(keys_with_list):
                column_json[k] = keys_with_list_values[idx]
            yield column_json

    return itertools.chain.from_iterable(map(iter_expanded_dicts, seq))


# URLs


def dimensions_to_dimension_params(dimensions):
    """Transform a single query string parameter (named "dimensions") to many parameters (named "dimension").

    >>> dimensions_to_dimension_params('k1:v1')
    ['k1:v1']
    >>> dimensions_to_dimension_params('k1:v1,')
    ['k1:v1']
    >>> dimensions_to_dimension_params('k1:v1a,v1b')
    ['k1:v1a', 'k1:v1b']
    >>> dimensions_to_dimension_params('k1:v1;k2:v2')
    ['k1:v1', 'k2:v2']
    >>> dimensions_to_dimension_params('k1:v1;k2:v2;')
    ['k1:v1', 'k2:v2']
    >>> dimensions_to_dimension_params('k1:v1a,v1b;k2:v2a,v2b')
    ['k1:v1a', 'k1:v1b', 'k2:v2a', 'k2:v2b']
    """
    def iter_dimension_params():
        for dimension in list(filter(None, dimensions.split(';'))):
            k, values = list(filter(None, dimension.split(':')))
            for v in list(filter(None, values.split(','))):
                yield '{}:{}'.format(k, v)

    return list(iter_dimension_params())


def website_url_to_api_url(url, api_base_url=default_api_base_url):
    """Transform the URL of a dataset on DB.nomics website to the URL in the Web API
    endpoint `/{provider_code}/{dataset_code}`.

    >>> website_url_to_api_url("https://next.nomics.world/Eurostat/ei_bsin_q_r2")
    'https://api.next.nomics.world/Eurostat/ei_bsin_q_r2'
    >>> website_url_to_api_url("https://localhost:8000/Eurostat/ei_bsin_q_r2", api_base_url="http://localhost:5000")
    'http://localhost:5000/Eurostat/ei_bsin_q_r2'
    >>> website_url_to_api_url("https://next.nomics.world/Eurostat/ei_bsin_q_r2?dimensions=indic%3ABS-FLP6-PC")
    'https://api.next.nomics.world/Eurostat/ei_bsin_q_r2?dimension=indic%3ABS-FLP6-PC'
    >>> website_url_to_api_url("https://next.nomics.world/Eurostat/ei_bsin_q_r2?dimensions=indic%3ABS-FLP2-PC%2CBS-FLP6-PC")
    'https://api.next.nomics.world/Eurostat/ei_bsin_q_r2?dimension=indic%3ABS-FLP2-PC&dimension=indic%3ABS-FLP6-PC'
    """
    api_base_url_parts = urlsplit(api_base_url)
    url_split_result = urlsplit(url)
    url_parts = list(url_split_result)
    url_parts[0] = api_base_url_parts[0]
    url_parts[1] = api_base_url_parts[1]
    url_parts[2] = url_split_result.path
    query = parse_qs(url_split_result.query)
    dimensions = query.get("dimensions")
    if dimensions:
        if len(dimensions) > 1:
            raise ValueError(
                "URL should contain zero or one query string parameter named 'dimensions', but {} were found.".format(len(dimensions)))
        url_parts[3] = "&".join(
            'dimension=' + quote(dimension_param)
            for dimension_param in dimensions_to_dimension_params(dimensions[0])
        )
    return urlunsplit(url_parts)
