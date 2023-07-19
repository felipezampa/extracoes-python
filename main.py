import psycopg2 as pg 
import os
from dotenv import load_dotenv
import datetime
import members, labels, boards, lists, cards, checklists


# Initial time of the program
start_time = datetime.datetime.now()
# Load environment variables from the .env file
load_dotenv()
# Assign the values to variables
token = os.getenv("token")
key = os.getenv("key")
organization = os.getenv("organization")
# Connect in the database
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
# End time of the program
end_time = datetime.datetime.now()
# Calculate the time of the execution
execution_time = end_time - start_time

print(f"Tempo de execução: {execution_time}")