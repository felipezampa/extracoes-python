import requests

def extract_cards(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_cards -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.cards (
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
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_cards -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      # Neste exemplo, a subconsulta SELECT 1 FROM cards WHERE id = %s 
      # verifica se já existe um registro com o mesmo id na tabela "cards". 
      # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
      cursor.execute(
        '''
            INSERT INTO cards (id, nome, descricao, dt_inicio, dt_entrega, terminado, id_board, id_list)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM cards WHERE id = %s
            )
        ''', 
        (item['id'], item['name'], item['desc'], item['start'], item['due'], item['dueComplete'], item['idBoard'], item['idList'], item['id'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_cards -> Extração concluída com sucesso!')

def extract_cards_labels(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_cards_labels -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.cards_labels (
        id_board text,
        id_card text,
        id_label text
      );
  ''')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_cards_labels -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      labels = item['idLabels']

      for label in labels:        
        # Neste exemplo, a subconsulta SELECT 1 FROM cards_labels WHERE id = %s 
        # verifica se já existe um registro com o mesmo id na tabela "cards_labels". 
        # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
        cursor.execute(
          '''
              INSERT INTO cards_labels (id_board, id_card, id_label)
              SELECT %s, %s, %s
              WHERE NOT EXISTS (
                  SELECT 1 FROM cards_labels WHERE id_board = %s AND id_card = %s AND id_label = %s 
              )
          ''', 
          (str(id), item['id'], label, str(id), item['id'], label)
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_cards_labels -> Extração concluída com sucesso!')

def extract_cards_checklists(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_cards_checklists -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.cards_checklists (
        id_board text,
        id_card text,
        id_checklist text
      );
  ''')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/cards?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_cards_checklists -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      checklists = item['idChecklists']

      for check in checklists:        
        # Neste exemplo, a subconsulta SELECT 1 FROM cards_checklists WHERE id = %s 
        # verifica se já existe um registro com o mesmo id na tabela "cards_checklists". 
        # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
        cursor.execute(
          '''
              INSERT INTO cards_checklists (id_board, id_card, id_checklist)
              SELECT %s, %s, %s
              WHERE NOT EXISTS (
                  SELECT 1 FROM cards_checklists WHERE id_board = %s AND id_card = %s AND id_checklist = %s 
              )
          ''', 
          (str(id), item['id'], check, str(id), item['id'], check)
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_cards_checklists -> Extração concluída com sucesso!')