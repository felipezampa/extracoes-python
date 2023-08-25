import requests

def extract_boards(token,key,organization,engine):
  """
    Extrai informações de quadros (boards) do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `organization: Organização do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  tabela = 'boards'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/boards?key={}&token={}'.format(organization, key, token)

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
        fechado boolean,
        data_fechamento date,
        id_criador text
      );
  ''')

  cursor.execute(f'TRUNCATE TABLE {tabela}')
  # Insere os dados na tabela
  print(f'{tabela} -> Inserção dos dados')
  for item in data:
    cursor.execute(
      f'''
          INSERT INTO {tabela} (id, nome, fechado, data_fechamento, id_criador)
          VALUES (%s, %s, %s, %s, %s)
      ''', 
      (item['id'], item['name'], item['closed'], item['dateClosed'], item['idMemberCreator'])
    )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')

def extract_boards_members(token,key,organization,engine):
  """
    Extrai informações de quadros (boards) e membros(members) do Trello e
    insere essas informações em uma tabela matriz do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `organization: Organização do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  tabela='boards_members'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/boards?key={}&token={}'.format(organization, key, token)

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
        id_board text,
        id_membro text,
        tipo_membro text,
        inativo boolean
      );
  ''')

  cursor.execute(f'TRUNCATE TABLE {tabela}')
  # Insere os dados na tabela
  print(f'{tabela} -> Inserção dos dados')
  for item in data:
    memberships = item['memberships']

    for member in memberships: 
      cursor.execute(
        f'''
            INSERT INTO {tabela} (id, id_board, id_membro, tipo_membro, inativo)
            VALUES (%s, %s, %s, %s, %s)
        ''', 
        (member['id'], item['id'], member['idMember'], member['memberType'], member['deactivated'])
      )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')
    
def get_all_boards(engine):
  """
    Recupera todos os registros de de quadros (boards) da tabela do PostgreSQL.

    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  print('Limpando dados dos boards')
  cursor = engine.cursor()
  cursor.execute('''
                 call public.clean_boards();
                 SELECT id FROM boards
                 ''')
  # A função fetchall() é usada para recuperar todas as linhas retornadas pela consulta
  results = cursor.fetchall()
  idBoards = [row[0] for row in results]
  cursor.close()

  return idBoards 
