import requests

def extract_lists(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_lists -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.lists (
        id text NULL,
        nome text NULL,
        fechado text NULL,
        id_board text NULL
      );
  ''')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/lists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_lists -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      # Neste exemplo, a subconsulta SELECT 1 FROM lists WHERE id = %s 
      # verifica se já existe um registro com o mesmo id na tabela "lists". 
      # A cláusula WHERE NOT EXISTS impede a inserção dos dados se o registro já existir. evitando dados duplicados
      cursor.execute(
        '''
            INSERT INTO lists (id, nome, fechado, id_board)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM lists WHERE id = %s
            )
        ''', 
        (item['id'], item['name'], item['closed'], item['idBoard'], item['id'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_lists -> Extração concluída com sucesso!')
  