import psycopg2 as pg 
import os
from dotenv import load_dotenv
import datetime
import members, labels, boards, lists, cards, checklists


# Tempo do inicio da execução
start_time = datetime.datetime.now()
# Carrega as variaveis de ambiente do arquivo .env
load_dotenv()
# Atribui os valores do .env a variaveis locais
token = os.getenv("token")
key = os.getenv("key")
organization = os.getenv("organization")
# Conecta no postgres
engine = pg.connect(
    dbname=os.getenv("dbname"), user=os.getenv("user"), host=os.getenv("host"), port=os.getenv("port"), password=os.getenv("password")
)

try:
  members.extract_members(token, key, organization, engine)
  boards.extract_boards(token, key, organization, engine)
  boards.extract_boards_members(token,key,organization,engine)
  
  boards_trello = boards.get_all_boards(engine)

  lists.extract_lists(token, key, engine, boards_trello)
  cards.extract_cards(token, key, engine, boards_trello)
  cards.extract_cards_checklists(token, key, engine, boards_trello)
  cards.extract_cards_labels(token, key, engine, boards_trello)
  labels.extract_labels(token, key, engine, boards_trello)
  checklists.extract_checklists(token, key, engine, boards_trello)
  checklists.extract_checklists_items(token, key, engine, boards_trello)
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