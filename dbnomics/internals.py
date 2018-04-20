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


import logging

import requests
import semver

api_min_version = '0.14.0'
api_max_version = '0.18.0'
log = logging.getLogger(__name__)


def api_version_matches(api_version):
    return semver.match(api_version, ">=" + api_min_version) and \
        semver.match(api_version, "<" + api_max_version)


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
        raise ValueError('Could not parse JSON payload from URL {!r} because: {}. Response text: {}'.format(
            page_url, exc, response.text))

    if not response.ok:
        raise ValueError("Could not fetch data from URL {!r} because: {}".format(
            page_url, response_json['error_description']))

    api_version = response_json['_meta']['python_project_version']
    if not api_version_matches(api_version):
        raise ValueError('Web API version is {!r}, but this version of the Python client is expecting >= {}, < {}'.format(
            api_version, api_min_version, api_max_version))

    error_description = response_json.get('error_description')
    if error_description is not None:
        raise ValueError('Could not find "series" key in response JSON payload. URL = {!r}, received: {!r}'.format(
            page_url, response_json))

    series_json_page = response_json.get('series')
    if series_json_page is None:
        raise ValueError('Could not find "series" key in response JSON payload. URL = {!r}, received: {!r}'.format(
            page_url, response_json))
    assert series_json_page['offset'] == offset, (series_json_page['offset'], offset)

    return response_json
