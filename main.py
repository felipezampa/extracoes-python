#%%
import requests
import pandas as pd
import psycopg2 as pg 
import members
import boards

# Variaveis de conexao
with open('credentials.txt', 'r') as file:
    lines = file.readlines()
# Create an empty dictionary to store the variables
variables = {}
# Iterate over the lines and extract the variables
for line in lines:
    # Remove any leading/trailing whitespace and split the line at the "=" sign
    variable, value = line.strip().split('=')
    variables[variable] = value

token = variables['token']
key = variables['key']
organization = variables['organization']
engine = pg.connect("dbname='trello' user='postgres' host='localhost' port='5432' password='postgres'")
try:
  members.extractMembers(token,key,organization,engine)
  boards.extractBoards(token,key,organization,engine)
  # boards.extractMembersBoards(token,key,organization,engine)
except Exception as e:
  print('-----------------  ERRO  -----------------')
  print(e)
finally:
  engine.close()