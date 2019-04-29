
#### Import stuff ####
import pandas as pd
# import numpy as np
import dbnomics

# set some vars
provider = 'OECD/MEI'
series_id = ['GBR.CP010000.CTGY.M', 'FRA.CP010000.CTGY.M']
urllink = 'https://api.db.nomics.world/v22/series/AMECO/ZUTN/CYP.1.0.0.0.ZUTN?observations=1'

# retrieve series, check errors
# new random series
foo = dbnomics.fetch_series_by_api_link(urllink, max_nb_series=2)

foo.loc[:,"value"].head(20)

# series with known errors
tables = [
    dbnomics.fetch_series(provider_code=provider, dataset_code=s)
    for s in series_id
]

dfipy = tables[0]
print(dfipy.iloc[53])

# if dfipy['value'].iloc[53] == "NA":
# 	print('\n+++++++++++++++SERPENTEPOLIZIOTTOPIEDIPIATTI+++++++++++++++\n') 