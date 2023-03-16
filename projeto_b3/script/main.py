# aqui comeca seu codigo
#%% 
import pandas as pd
import requests
from bs4 import BeautifulSoup
#%% 
url = "https://www.google.com"
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html, "html.parser")
