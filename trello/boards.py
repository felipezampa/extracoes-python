import requests

def extract_boards(token,key,organization,engine):
  print('------------------------------------------')
  print('extract_boards -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/boards?key={}&token={}'.format(organization, key, token)

  # Recupera as informações da url
  response = requests.get(url)
  # Converte a resposta para um objeto JSON
  data = response.json()  
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.boards (
        id text,
        nome text,
        fechado boolean,
        data_fechamento date,
        id_criador text
      );
  ''')
  cursor.execute('TRUNCATE TABLE boards')
  # Insere os dados na tabela
  print('extract_boards -> Inserção dos dados')
  for item in data:
    # Neste exemplo, a subconsulta SELECT 1 FROM boards WHERE id = %s 
    # verifica se já existe um registro com o mesmo id na tabela "boards". 
    # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
    cursor.execute(
      '''
          INSERT INTO boards (id, nome, fechado, data_fechamento, id_criador)
          VALUES (%s, %s, %s, %s, %s)
      ''', 
      (item['id'], item['name'], item['closed'], item['dateClosed'], item['idMemberCreator'])
    )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print('extract_boards -> Extração concluída com sucesso!')

def extract_boards_members(token,key,organization,engine):
  print('------------------------------------------')
  print('extract_boards_members -> Inicio da leitura da URL')
  url = 'https://api.trello.com/1/organizations/{}/boards?key={}&token={}'.format(organization, key, token)

  # Recupera as informações da url
  response = requests.get(url)
  # Converte a resposta para um objeto JSON
  data = response.json()  
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.boards_members (
        id text,
        id_board text,
        id_membro text,
        tipo_membro text,
        inativo boolean
      );
  ''')
  # Insere os dados na tabela
  print('extract_boards_members -> Inserção dos dados')
  cursor.execute('TRUNCATE TABLE boards_members')
  for item in data:
    memberships = item['memberships']

    for member in memberships: 
      cursor.execute(
        '''
            INSERT INTO boards_members (id, id_board, id_membro, tipo_membro, inativo)
            VALUES (%s, %s, %s, %s, %s)
        ''', 
        (member['id'], item['id'], member['idMember'], member['memberType'], member['deactivated'])
      )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print('extract_boards_members -> Extração concluída com sucesso!')
    
def get_all_boards(engine):
  cursor = engine.cursor()
  cursor.execute('SELECT id FROM boards')
  # A função fetchall() é usada para recuperar todas as linhas retornadas pela consulta
  results = cursor.fetchall()
  idBoards = [row[0] for row in results]
  cursor.close()

  return idBoards 
