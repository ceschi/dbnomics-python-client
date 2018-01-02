# DB.nomics Python client

Access DB.nomics time series from Python.

This project relies on [Python Pandas](https://pandas.pydata.org/).

## Demo

A demo is available thanks to the [Binder project](https://mybinder.org/).

To launch the demo, click here: [![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/cbenz/dbnomics-python-client/master?filepath=index.ipynb) then wait a couple of seconds to be redirected to a Jupyter Notebook home page, displaying a list of files. In this list, click on `index.ipynb`. Then you'll be in a demo notebook, where you'll be able to play with the DB.nomics Python client.

Unfortunately, Binder is only compatible with GitHub for now, and DB.nomics is hosted on its own [GitLab platform](https://git.nomics.world/). That's why we created a mirror of this project [on GitHub](https://github.com/cbenz/dbnomics-python-client), but the real home is [on DB.nomics GitLab](https://git.nomics.world/dbnomics/dbnomics-python-client).

## Install

```sh
git clone https://git.nomics.world/dbnomics/dbnomics-python-client.git
cd dbnomics-python-client
pip install --editable .
```

## Usage

The `fetch_dataframe` and `fetch_dataframe_from_url` functions download time series from the DB.nomics Web API and expose them as a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html).

The first takes individual parameters for `provider_code`, `dataset_code` and `series_code`, and the second takes an URL corresponding to the `/dataframe` endpoint of the Web API (see [API documentation](https://api.next.nomics.world/apidocs)).

The second version is useful especially in conjunction with the Web UI: on a dataset page (ie https://next.nomics.world/Eurostat/rd_p_persage) there is a "Dataframe URL" link which you can copy-paste as an argument. See the examples below.

```python
from dbnomics_client import fetch_dataframe, fetch_dataframe_from_url

# Fetch a single time series, individual params version
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

# Fetch all time series of a dataset, URL version (to use with the Web UI)
fetch_dataframe_from_url("https://next.nomics.world/dataframe/Eurostat/rd_p_persage")
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