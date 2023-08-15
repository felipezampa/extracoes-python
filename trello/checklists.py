import requests

def extract_checklists(token,key,engine,idBoards):
  """
    Extrai informações de checklists do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela = 'checklists'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id text,
        nome text,
        id_board text,
        id_card text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/checklists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    
    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      cursor.execute(
        f'''
            INSERT INTO {tabela} (id, nome, id_board, id_card)
            VALUES (%s, %s, %s, %s)
        ''', 
        (item['id'], item['name'], item['idBoard'], item['idCard'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')

def extract_checklists_items(token,key,engine,idBoards):
  """
    Extrai informações de checklists e os itens de cada checklist do Trello e
    insere essas informações em uma tabela matriz do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela = 'checklists_items'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id text,
        nome text,
        estado text,
        dt_entrega timestamp,
        id_membro text, 
        id_checklist text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/checklists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for check in data:
      check_items = check['checkItems']

      for item in check_items:
        cursor.execute(
          f'''
              INSERT INTO {tabela} (id, nome, estado, dt_entrega, id_membro, id_checklist)
              VALUES ( %s, %s, %s, %s, %s, %s)
          ''', 
          (item['id'], item['name'], item['state'], item['due'], item['idMember'], item['idChecklist'])
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')
