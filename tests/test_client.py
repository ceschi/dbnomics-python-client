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


from dbnomics import fetch_series, fetch_series_by_api_link


def test_fetch_series_by_code():
    df = fetch_series('AMECO', 'ZUTN', 'EA19.1.0.0.0.ZUTN')

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "AMECO", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "ZUTN", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) == 1, df
    assert series_codes[0] == "EA19.1.0.0.0.ZUTN", df


def test_fetch_series_by_code_mask():
    df = fetch_series("IMF", "CPI", "M.FR+DE.PCPIEC_IX+PCPIA_IX")

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "IMF", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "CPI", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) == 4, df


def test_fetch_series_by_id():
    df = fetch_series('AMECO/ZUTN/EA19.1.0.0.0.ZUTN')

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "AMECO", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "ZUTN", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) == 1, df
    assert series_codes[0] == "EA19.1.0.0.0.ZUTN", df


def test_fetch_series_by_ids_in_same_dataset():
    df = fetch_series([
        'AMECO/ZUTN/EA19.1.0.0.0.ZUTN',
        'AMECO/ZUTN/DNK.1.0.0.0.ZUTN',
    ])

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "AMECO", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "ZUTN", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) == 2, df
    assert series_codes[0] == "EA19.1.0.0.0.ZUTN", df
    assert series_codes[1] == "DNK.1.0.0.0.ZUTN", df


def test_fetch_series_by_ids_in_different_datasets():
    df = fetch_series([
        'AMECO/ZUTN/EA19.1.0.0.0.ZUTN',
        'BIS/PP-SS/Q.AU.N.628',
    ])

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 2, df
    assert provider_codes[0] == "AMECO", df
    assert provider_codes[1] == "BIS", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 2, df
    assert dataset_codes[0] == "ZUTN", df
    assert dataset_codes[1] == "PP-SS", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) == 2, df
    assert series_codes[0] == "EA19.1.0.0.0.ZUTN", df
    assert series_codes[1] == "Q.AU.N.628", df


def test_fetch_series_by_dimension():
    df = fetch_series("WB", "DB", dimensions={
        "country": ["FR", "IT", "ES"],
        "indicator": ["IC.REG.COST.PC.FE.ZS.DRFN"],
    })

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "WB", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "DB", df

    series_codes = df["series_code"].unique()
    assert len(series_codes), df


def test_fetch_series_of_dataset():
    df = fetch_series('AMECO', 'ZUTN')

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "AMECO", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "ZUTN", df

    series_codes = df["series_code"].unique()
    assert len(series_codes) > 1, df


def test_fetch_series_by_api_link():
    df = fetch_series_by_api_link(
        "https://api.dev.db.nomics.world/v22/series/BIS/PP-SS?dimensions=%7B%22FREQ%22%3A%5B%22Q%22%5D%2C%22REF_AREA%22%3A%5B%22AU%22%5D%7D&observations=1")

    provider_codes = df["provider_code"].unique()
    assert len(provider_codes) == 1, df
    assert provider_codes[0] == "BIS", df

    dataset_codes = df["dataset_code"].unique()
    assert len(dataset_codes) == 1, df
    assert dataset_codes[0] == "PP-SS", df

    series_codes = df["series_code"].unique()
    assert len(series_codes), df
