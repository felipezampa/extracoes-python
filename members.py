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
          id text NULL,
          nome text NULL,
          username text NULL
      );
    '''
  )

  # Insere os dados na tabela
  print('extract_members -> Inserção dos dados')
  for item in data:
    # Neste exemplo, a subconsulta SELECT 1 FROM members WHERE id = %s 
    # verifica se já existe um registro com o mesmo id na tabela "members". 
    # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
    cursor.execute('''
        INSERT INTO members (id, nome, username)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM members WHERE id = %s
        )
      ''',
      (item['id'], item['fullName'], item['username'], item['id'])
    )

  # Confirma as alterações no banco de dados
  engine.commit()

  # Fecha o cursor e a conexão com o banco de dados
  cursor.close()
  print('extract_members -> Extração concluída com sucesso!')
