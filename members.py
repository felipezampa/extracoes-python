import requests
import pandas as pd

def extract_members(token,key,organization,engine):
  print('------------------------------------------')
  print('extract_members -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/members?key={}&token={}'.format(organization, key, token)
  # Recupera as informações da url
  response = requests.get(url)
  # Converte a resposta para um objeto JSON
  data = response.json()  
  df = pd.json_normalize(data)  
  
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.members (
          id text,
          nome text,
          username text
      );
    '''
  )

  # Insere os dados na tabela
  print('extract_members -> Inserção dos dados')
  cursor.execute('TRUNCATE TABLE members')
  for item in data:
    cursor.execute('''
        INSERT INTO members (id, nome, username)
        VALUES( %s, %s, %s )
      ''',
      (item['id'], item['fullName'], item['username'])
    )

  # Confirma as alterações no banco de dados
  engine.commit()

  # Fecha o cursor e a conexão com o banco de dados
  cursor.close()
  print('extract_members -> Extração concluída com sucesso!')
