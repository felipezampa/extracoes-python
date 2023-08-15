import requests

def extract_members(token,key,organization,engine):
  """
    Extrai informações de membros (members) do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `organization: Organização do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  tabela = 'members'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/members?key={}&token={}'.format(organization, key, token)
 
  # Recupera as informações da url
  response = requests.get(url)
  # Converte a resposta para um objeto JSON
  data = response.json()  
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
          id text,
          nome text,
          username text
      );
    '''
  )

  cursor.execute(f'TRUNCATE TABLE {tabela}')
  # Insere os dados na tabela
  print(f'{tabela} -> Inserção dos dados')
  for item in data:
    cursor.execute(f'''
        INSERT INTO {tabela} (id, nome, username)
        VALUES( %s, %s, %s )
      ''',
      (item['id'], item['fullName'], item['username'])
    )

  # Confirma as alterações no banco de dados
  engine.commit()

  # Fecha o cursor e a conexão com o banco de dados
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')
