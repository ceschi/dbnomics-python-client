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

import logging

import pandas as pd
import requests


default_api_base_url = 'https://api.next.nomics.world'

log = logging.getLogger(__name__)


def fetch_dataframe(provider_code, dataset_code, series_code='', api_base_url=default_api_base_url, limit=None):
    """Download a dataframe from DB.nomics Web API, giving individual parameters. A dataframe contains many time series.

    Return a Python Pandas `DataFrame`.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.
    """
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    dataframe_url = api_base_url + '/dataframe/{}/{}?series_code={}'.format(provider_code, dataset_code, series_code)
    return fetch_dataframe_from_url(dataframe_url, limit=limit)


def fetch_dataframe_from_url(url, limit=None):
    """Download a dataframe from DB.nomics Web API, giving an URL. A dataframe contains many time series.

    Return a Python Pandas `DataFrame`.

    If the number of time series is greater than the limit allowed by the Web API, this function reconstitutes data
    by making many HTTP requests.
    """
    dataframe_json_data = []
    while True:
        dataframe_json = fetch_dataframe_page(url, offset=len(dataframe_json_data))
        dataframe_json_data.extend(dataframe_json['data'])
        nb_series = len(dataframe_json_data)
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
    response_json = requests.get(page_url).json()
    dataframe_json = response_json.get('dataframe')
    if dataframe_json is None:
        raise ValueError('Could not find "dataframe" key in response JSON payload. URL = {!r}, received: {!r}'.format(
            page_url, response_json))
    assert dataframe_json['type'] == 'dict', dataframe_json['type']
    assert dataframe_json['orient'] == 'columns', dataframe_json['orient']
    assert dataframe_json['offset'] == offset, (dataframe_json['offset'], offset)
    return dataframe_json
