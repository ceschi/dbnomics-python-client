## 0.4.0 -> 1.0.0

Breaking changes in Python API:

- `fetch_series` function: rename `code_mask` to `series_code`. Before it could only be a mask. Now it's possible to use it as a normal series code or a mask.

## 0.3.0 -> 0.4.0

Breaking changes in column names:

- Rename `period` to `original_period`, and `period_start_day` to `period`.

## 0.2.1 -> 0.3.0

Breaking changes in Python API:

- Remove `period_to_datetime` keyword argument from functions `fetch_series` and `fetch_series_by_api_link`. A new column named `period_start_day` has been added to the `DataFrame`, which contains the first day of the `period` column. This has been done because some periods formats are not understood by Pandas, for example "2018-B2" which corresponds to march and april 2018. See also: https://git.nomics.world/dbnomics/dbnomics-data-model/ for a list of periods formats.