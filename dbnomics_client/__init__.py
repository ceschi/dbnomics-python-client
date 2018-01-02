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

import pandas as pd
import requests
import semver


api_min_version = '0.9.0'
api_max_version = '0.10.0'
default_api_base_url = 'https://api.next.nomics.world'
default_limit = 3000

log = logging.getLogger(__name__)


def api_version_matches(api_version):
    return semver.match(api_version, ">=" + api_min_version) and \
        semver.match(api_version, "<" + api_max_version)


def fetch_dataframe(provider_code, dataset_code, series_code='', api_base_url=default_api_base_url, limit=default_limit):
    """Download a dataframe from DB.nomics Web API, giving individual parameters. A dataframe contains many time series.

    Return a Python Pandas `DataFrame`.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.

    By default this function sets a limit of 3000 series, via the `dbnomics_client.default_limit` constant.
    Pass `limit=None` explicitly to download an unlimited number of series.

    The DB.nomics Web API base URL can be customized by passing a value to the `api_base_url` parameter.
    This will probably never be useful, unless somebody deploys a new instance of DB.nomics under another domain name.
    """
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    dataframe_url = api_base_url + '/dataframe/{}/{}?series_code={}'.format(provider_code, dataset_code, series_code)
    return fetch_dataframe_from_url(dataframe_url, limit=limit)


def fetch_dataframe_from_url(url, limit=default_limit):
    """Download a dataframe from DB.nomics Web API, giving an URL. A dataframe contains many time series.

    Return a Python Pandas `DataFrame`.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.

    By default this function sets a limit of 3000 series, via the `dbnomics_client.default_limit` constant.
    Pass `limit=None` explicitly to download an unlimited number of series.
    """
    dataframe_json_data = []
    while True:
        nb_series = len(set(map(lambda e: e['code'], dataframe_json_data)))
        dataframe_json = fetch_dataframe_page(url, offset=nb_series)
        dataframe_json_data.extend(dataframe_json['data'])
        if limit is not None and nb_series >= limit:
            dataframe_json_data = dataframe_json_data[:limit]
            break
        if nb_series == dataframe_json['num_found']:
            break
    return pd.DataFrame(dataframe_json_data)


def fetch_dataframe_page(dataframe_url, offset):
    page_url = '{}{}offset={}'.format(
        dataframe_url,
        '&' if '?' in dataframe_url else '?',
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
        raise ValueError(response_json['message'])
    api_version = response_json['_meta']['python_project_version']
    if not api_version_matches(api_version):
        raise ValueError('Web API version is {!r}, but this version of the Python client is expecting >= {}, < {}'.format(
            api_version, api_min_version, api_max_version))
    dataframe_json = response_json.get('dataframe')
    if dataframe_json is None:
        raise ValueError('Could not find "dataframe" key in response JSON payload. URL = {!r}, received: {!r}'.format(
            page_url, response_json))
    assert dataframe_json['type'] == 'dict', dataframe_json['type']
    assert dataframe_json['orient'] == 'columns', dataframe_json['orient']
    assert dataframe_json['offset'] == offset, (dataframe_json['offset'], offset)
    dataframe_json['data'] = list(iter_dataframe_dicts(dataframe_json['data']))
    return dataframe_json


def iter_dataframe_dicts(seq):
    """Transform the `list` of `dict` received by DB.nomics Web API to a `list` of `dict` compatible with Python Pandas,
    in order to build a `DataFrame`.

    >>> list(iter_dataframe_dicts([
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
    ... ]))
    [{'code': 's1', 'FREQ': 'M', 'period': '2010', 'value': 1}, {'code': 's1', 'FREQ': 'M', 'period': '2011', 'value': 2}, {'code': 's1', 'FREQ': 'M', 'period': '2012', 'value': 3}, {'code': 's2', 'FREQ': 'Q', 'period': '2010', 'value': 999}]
    """
    def iter_dataframe_dicts(d):
        keys_with_list = [
            k
            for k, v in d.items()
            if isinstance(v, list)
        ]
        for keys_with_list_values in zip(*(d[k] for k in keys_with_list)):
            dataframe_dict = d.copy()
            for idx, k in enumerate(keys_with_list):
                dataframe_dict[k] = keys_with_list_values[idx]
            yield dataframe_dict

    return itertools.chain.from_iterable(map(iter_dataframe_dicts, seq))
