import psycopg2 as pg 
import os
from dotenv import load_dotenv
import datetime
import fields, deals, users


# Tempo do inicio da execução
start_time = datetime.datetime.now()
# Carrega as variaveis de ambiente do arquivo .env
load_dotenv()
# Atribui os valores do .env a variaveis locais
key = os.getenv("key")
# Conecta no postgres
engine = pg.connect(
    dbname=os.getenv("dbname"), user=os.getenv("user"), host=os.getenv("host"), port=os.getenv("port"), password=os.getenv("password")
)

try:
  # Chamada dos metodos de extracao
  fields.extract_fields(key,engine)
  deals.extract_deals(key,engine)
  users.extract_users(key,engine)
except Exception as e:
  print('-----------------  ERRO  -----------------')
  print(e)
finally:
  print('-----------------  FIM  ------------------')
  engine.close()
# Tempo do fim da execução
end_time = datetime.datetime.now()
# Calcula o tempo de execução
execution_time = end_time - start_time

print(f"Tempo de execução: {execution_time}")