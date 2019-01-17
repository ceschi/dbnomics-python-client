# dbnomics-python-client -- Access DBnomics time series from Python
# By: Christophe Benz <christophe.benz@cepremap.org>
#
# Copyright (C) 2017-2018 Cepremap
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


"""Access DBnomics time series from Python."""


import json
import os
import urllib.parse

import pandas as pd
import requests

default_api_base_url = os.environ.get('API_URL') or 'https://api.db.nomics.world/v22'
default_max_nb_series = 50


class TooManySeries(Exception):
    def __init__(self, num_found, max_nb_series):
        self.num_found = num_found
        self.max_nb_series = max_nb_series
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


def fetch_series(provider_code=None, dataset_code=None, series_code=None, dimensions=None, series_ids=None,
                 max_nb_series=None, api_base_url=None):
    """Download time series from DBnomics. Filter series by different ways according to the given parameters.

    If not `None`, `dimensions` parameter must be a `dict` of dimensions (`list` of `str`), like so:
    `{"freq": ["A", "M"], "country": ["FR"]}`.

    If not `None`, `series_code` must be a `str`. It can be a series code (one series), or a "mask" (many series):
    - remove a constraint on a dimension, for example `M..PCPIEC_WT`;
    - enumerate many values for a dimension, separated by a '+', for example `M.FR+DE.PCPIEC_WT`;
    - combine these possibilities many times in the same SDMX filter.

    If the rightmost dimension value code is removed, then the final '.' can be removed too: `A.FR.` = `A.FR`.

    If not None, `series_ids` parameter must be a non-empty `list` of series IDs.
    A series ID is a string formatted like `provider_code/dataset_code/series_code`.

    Return a Python Pandas `DataFrame`.

    If `max_nb_series` is `None`, a default value of 50 series will be used.

    Examples:

    - fetch one series:
      fetch_series("AMECO/ZUTN/EA19.1.0.0.0.ZUTN")

    - fetch all the series of a dataset:
      fetch_series("AMECO", "ZUTN")

    - fetch many series from different datasets:
      fetch_series(["AMECO/ZUTN/EA19.1.0.0.0.ZUTN", "AMECO/ZUTN/DNK.1.0.0.0.ZUTN", "IMF/CPI/A.AT.PCPIT_IX"])

    - fetch many series from the same dataset, searching by dimension:
      fetch_series("AMECO", "ZUTN", dimensions={"geo": ["dnk"]})

    - fetch many series from the same dataset, searching by code mask:
      fetch_series("IMF", "CPI", series_code="M.FR+DE.PCPIEC_WT")
      fetch_series("IMF", "CPI", series_code=".FR.PCPIEC_WT")
      fetch_series("IMF", "CPI", series_code="M..PCPIEC_IX+PCPIA_IX")
    """
    # Parameters validation
    if api_base_url is None:
        api_base_url = default_api_base_url
    if api_base_url.endswith('/'):
        api_base_url = api_base_url[:-1]
    if dataset_code is None:
        if isinstance(provider_code, list):
            series_ids = provider_code
            provider_code = None
        elif isinstance(provider_code, str):
            series_ids = [provider_code]
            provider_code = None

    if provider_code is not None and not isinstance(provider_code, str):
        raise ValueError("`provider_code` parameter must be a string")
    if dataset_code is not None and not isinstance(dataset_code, str):
        raise ValueError("`dataset_code` parameter must be a string")
    if dimensions is not None and not isinstance(dimensions, dict):
        raise ValueError("`dimensions` parameter must be a dict")
    if series_code is not None and not isinstance(series_code, str):
        raise ValueError("`series_code` parameter must be a string")
    if series_ids is not None and (
        not isinstance(series_ids, list) or
        any(not isinstance(series_id, str) for series_id in series_ids)
    ):
        raise ValueError("`series_ids` parameter must be a list of strings")
    if api_base_url is not None and not isinstance(api_base_url, str):
        raise ValueError("`api_base_url` parameter must be a string")

    series_base_url = api_base_url + '/series'

    if dimensions is None and series_code is None and series_ids is None:
        if not provider_code or not dataset_code:
            raise ValueError("When you don't use `dimensions`, you must specifiy `provider_code` and `dataset_code`.")
        api_link = series_base_url + '/{}/{}?observations=1'.format(provider_code, dataset_code)
        return fetch_series_by_api_link(api_link, max_nb_series=max_nb_series)

    if dimensions is not None:
        if not provider_code or not dataset_code:
            raise ValueError("When you use `dimensions`, you must specifiy `provider_code` and `dataset_code`.")
        api_link = series_base_url + \
            '/{}/{}?observations=1&dimensions={}'.format(provider_code, dataset_code, json.dumps(dimensions))
        return fetch_series_by_api_link(api_link, max_nb_series=max_nb_series)

    if series_code is not None:
        if not provider_code or not dataset_code:
            raise ValueError("When you use `series_code`, you must specifiy `provider_code` and `dataset_code`.")
        api_link = series_base_url + '/{}/{}/{}?observations=1'.format(provider_code, dataset_code, series_code)
        return fetch_series_by_api_link(api_link, max_nb_series=max_nb_series)

    if series_ids is not None:
        if provider_code or dataset_code:
            raise ValueError("When you use `series_ids`, you must not specifiy `provider_code` nor `dataset_code`.")
        api_link = series_base_url + '?observations=1&series_ids={}'.format(','.join(series_ids))
        return fetch_series_by_api_link(api_link, max_nb_series=max_nb_series)

    raise ValueError("Invalid combination of function arguments")


def fetch_series_by_api_link(api_link, max_nb_series=None):
    """Fetch series given an "API link" URL.

    "API link" URLs can be found on DBnomics web site (https://db.nomics.world/) on dataset or series pages
    using "Download" buttons.

    Example:
      fetch_series(api_link="https://api.db.nomics.world/v22/series?provider_code=AMECO&dataset_code=ZUTN")
    """
    series_list = []
    offset = 0

    while True:
        response_json = _fetch_series_json_page(api_link, offset=offset)
        series_json_page = response_json["series"]

        num_found = series_json_page['num_found']
        if max_nb_series is None and num_found > default_max_nb_series:
            raise TooManySeries(num_found, max_nb_series)

        series_json_list = series_json_page['docs']
        for series_json in series_json_list:
            # Rename keys and convert str to datetime.
            period = series_json.pop("period", [])
            period_start_day = series_json.pop("period_start_day", [])
            series_json["original_period"] = period
            series_json["period"] = list(map(pd.to_datetime, period_start_day))

            # Flatten dimensions.
            dimensions = series_json.pop("dimensions", {})
            for dimension_code, dimension_value_code in dimensions.items():
                series_json[dimension_code] = dimension_value_code

            # Flatten observations attributes.
            observations_attributes = series_json.pop("observations_attributes", [])
            for attribute_code, attribute_value_code in observations_attributes:
                series_json[attribute_code] = attribute_value_code

            series_list.append(series_json)
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

    return pd.concat(map(pd.DataFrame, series_list)) \
        if series_list \
        else pd.DataFrame()


def _fetch_series_json_page(series_json_url, offset):
    page_url = '{}{}offset={}'.format(
        series_json_url,
        '&' if '?' in series_json_url else '?',
        offset,
    )

    response = requests.get(page_url)
    response_json = response.json()
    if not response.ok:
        message = response_json.get('message')
        raise ValueError("Could not fetch data from URL {!r} because: {}".format(page_url, message))

    series_json_page = response_json.get('series')
    if series_json_page is not None:
        assert series_json_page['offset'] == offset, (series_json_page['offset'], offset)

    return response_json
