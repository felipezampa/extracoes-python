import psycopg2 as pg 
import os
from dotenv import load_dotenv
import datetime
from extractions import members, labels, boards, lists, cards, checklists
from dim_facts import fact_trello, fact_checklists, fact_overviews, dim_boards, dim_cards, dim_lists, dim_vigencia 


# Tempo do inicio da execução
start_time = datetime.datetime.now()
# Carrega as variaveis de ambiente do arquivo .env
load_dotenv()
# Atribui os valores do .env a variaveis locais
token = os.getenv("TOKEN")
key = os.getenv("KEY")
organization = os.getenv("ORGANIZATION")
# Conecta no postgres
engine = pg.connect(
    dbname=os.getenv("POSTGRES_DBNAME"), user=os.getenv("POSTGRES_USER"), host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"), password=os.getenv("POSTGRES_PASSWORD")
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
  cards.clean_cards(engine)

  # Extrações em tabelas fato para copiar ao redshift
  fact_trello.extract(engine)
  fact_checklists.extract(engine)
  fact_overviews.extract(engine)
  dim_boards.extract(engine)
  dim_cards.extract(engine)
  dim_lists.extract(engine) 
  dim_vigencia.extract(engine)

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