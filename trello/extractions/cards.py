import requests

def extract_cards(token,key,engine,idBoards):
  """
    Extrai informações de cartões (cards) do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela = 'cards'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id text,
        nome text,
        descricao text,
        dt_inicio TIMESTAMP,
        dt_entrega TIMESTAMP,
        terminado boolean,
        id_board text,
        id_list text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 

    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      cursor.execute(
        f'''
            INSERT INTO {tabela} (id, nome, descricao, dt_inicio, dt_entrega, terminado, id_board, id_list)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', 
        (item['id'], item['name'], item['desc'], item['start'], item['due'], item['dueComplete'], item['idBoard'], item['idList'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')

def extract_cards_labels(token,key,engine,idBoards):
  """
    Extrai informações de cartões (cards) e etiquetas(labels) do Trello e
    insere essas informações em uma tabela matriz do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela = 'cards_labels'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id_board text,
        id_card text,
        id_label text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 

    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      labels = item['idLabels']

      for label in labels:        
        cursor.execute(
          f'''
              INSERT INTO {tabela} (id_board, id_card, id_label)
              VALUES (%s, %s, %s)
          ''', 
          (str(id), item['id'], label)
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')

def extract_cards_checklists(token,key,engine,idBoards):
  """
    Extrai informações de cartões (cards) e checklists do Trello e
    insere essas informações em uma tabela matriz do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela = 'cards_checklists'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id_board text,
        id_card text,
        id_checklist text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    
    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      checklists = item['idChecklists']

      for check in checklists:       
        cursor.execute(
          f'''
              INSERT INTO {tabela} (id_board, id_card, id_checklist)
              VALUES (%s, %s, %s)
          ''', 
          (str(id), item['id'], check)
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')

def clean_cards(engine):
  """
     Chama a procedure clean_cards() para tratar dados indesejados.
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  print('Limpando dados dos cards e labels')
  cursor = engine.cursor()
  cursor.execute('call public.clean_cards()')
  cursor.close()