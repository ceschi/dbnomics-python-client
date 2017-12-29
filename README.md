# DB.nomics Python client

Access DB.nomics time series from Python.

This project relies on [Python Pandas](https://pandas.pydata.org/).

## Install

```sh
git clone https://git.nomics.world/dbnomics/dbnomics-python-client.git
cd dbnomics-python-client
pip install --editable .
```

## Usage

The `fetch_dataframe` function downloads time series from the DB.nomics Web API and exposes them as a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html).

```python
from dbnomics_client import fetch_dataframe

# Fetch a single time series
fetch_dataframe(
    provider_code='Eurostat',
    dataset_code='rd_p_persage',
    series_code='A.HC.TOTAL.GOV.F.FR',
)

# Fetch many time series by passing a series code mask
fetch_dataframe(
    provider_code='Eurostat',
    dataset_code='rd_p_persage',
    series_code='A.HC.TOTAL.GOV.F.FR+DE',  # many specific countries
    # series_code='A.HC.TOTAL.GOV.F.',  # or all countries
)

# Fetch all time series of a dataset
fetch_dataframe(
    provider_code='Eurostat',
    dataset_code='nrg_134m',
)
```

DB.nomics Web API sets a limit over results and allows pagination. This Python client calls the Web API many times if necessary for you. Please be aware that this mechanism can make the `fetch_dataframe` function quite long to respond. You may use the `limit` parameter:

```python
dataframe = fetch_dataframe(
    provider_code='Eurostat',
    dataset_code='nrg_134m',
    limit=3000,
)
```

## Development

If you plan to use a local Web API, running on the port 5000, you'll need to use the `api_base_url` parameter of the `fetch_dataframe` function, like this:

```python
dataframe = fetch_dataframe(
    api_base_url='http://localhost:5000',
    provider_code='Eurostat',
    dataset_code='nrg_134m',
)
```