# aqui comeca seu codigo
#%% 
import pandas as pd
import requests
from bs4 import BeautifulSoup
#%%
codigobcb = 21620
url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json".format(codigobcb)
response = requests.get(url)
html = response.content
# %%
df = pd.read_json(url)
#%%
df = df.iloc[[-1]]
df['agrupador'] = 1
# df = df.drop('teste',axis="columns")
# %%
df.to_csv("C:/Safegold/Python/projeto_euro/data/euro.csv")
# %%
