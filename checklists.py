import requests

def extract_checklists(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_checklists -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.checklists (
        id text NULL,
        nome text NULL,
        id_board text NULL,
        id_card text NULL
      );
  ''')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/checklists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_checklists -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      # Neste exemplo, a subconsulta SELECT 1 FROM checklists WHERE id = %s 
      # verifica se já existe um registro com o mesmo id na tabela "checklists". 
      # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
      cursor.execute(
        '''
            INSERT INTO checklists (id, nome, id_board, id_card)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM checklists WHERE id = %s
            )
        ''', 
        (item['id'], item['name'], item['idBoard'], item['idCard'], item['id'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_checklists -> Extração concluída com sucesso!')

def extract_checklists_items(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_checklists_items -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.checklists_items (
        id text NULL,
        nome text NULL,
        estado text NULL,
        dt_entrega text NULL,
        id_membro text NULL, 
        id_checklist text NULL
      );
  ''')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/checklists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_checklists_items -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for check in data:
      check_items = check['checkItems']

      for item in check_items:
        # Neste exemplo, a subconsulta SELECT 1 FROM checklists_items WHERE id = %s 
        # verifica se já existe um registro com o mesmo id na tabela "checklists_items". 
        # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
        cursor.execute(
          '''
              INSERT INTO checklists_items (id, nome, estado, dt_entrega, id_membro, id_checklist)
              SELECT %s, %s, %s, %s, %s, %s
              WHERE NOT EXISTS (
                  SELECT 1 FROM checklists_items WHERE id = %s
              )
          ''', 
          (item['id'], item['name'], item['state'], item['due'], item['idMember'], item['idChecklist'], item['id'])
        )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_checklists_items -> Extração concluída com sucesso!')
