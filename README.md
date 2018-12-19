# DBnomics Python client

Access DBnomics time series from Python.

This project relies on [Python Pandas](https://pandas.pydata.org/).

## Demo

A demo is available thanks to the [Binder project](https://mybinder.org/).

To launch the demo, click here: [![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/dbnomics/dbnomics-python-client/master?filepath=index.ipynb) then wait a couple of seconds to be redirected to a Jupyter Notebook home page, displaying a list of files. In this list, click on `index.ipynb`. Then you'll be in a demo notebook, where you'll be able to play with the DBnomics Python client.

Unfortunately, Binder is only compatible with GitHub for now, and DBnomics is hosted on its own [GitLab platform](https://git.nomics.world/). That's why we created a mirror of this project [on GitHub](https://github.com/dbnomics/dbnomics-python-client), but the real home is [on DBnomics GitLab](https://git.nomics.world/dbnomics/dbnomics-python-client).

## Install

```sh
git clone https://git.nomics.world/dbnomics/dbnomics-python-client.git
cd dbnomics-python-client
pip install --editable .
```

## Development

If you plan to use a local Web API, running on the port 5000, you'll need to use the `api_base_url` parameter of the `fetch_*` functions, like this:

```python
dataframe = fetch_series(
    api_base_url='http://localhost:5000',
    provider_code='AMECO',
    dataset_code='ZUTN',
)
```

Or set the default API URL by [monkey-patching](https://en.wikipedia.org/wiki/Monkey_patch) the `dbnomics` module, like this:

```python
import dbnomics
dbnomics.default_api_base_url = "http://localhost:5000"
```

## Tests

Run tests:

```bash
pytest tests/test_client.py

# Specify an alterate API URL
API_URL=http://localhost:5000 pytest tests/test_client.py
```
