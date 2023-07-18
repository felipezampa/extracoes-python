import requests

def extract_cards(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_cards -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.cards (
        id text NULL,
        nome text NULL,
        descricao text NULL,
        dt_inicio text NULL,
        dt_entrega text NULL,
        dt_terminado text NULL,
        fechado text NULL,
        id_board text NULL,
        id_list text NULL
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
            INSERT INTO cards (id, nome, descricao, dt_inicio, dt_entrega, dt_terminado, fechado, id_board, id_list)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM cards WHERE id = %s
            )
        ''', 
        (item['id'], item['name'], item['desc'], item['start'], item['due'], item['dueComplete'], item['closed'], item['idBoard'], item['idList'], item['id'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_cards -> Extração concluída com sucesso!')

def extract_cards_labels():
  print('fazer N - N para ter os cards e labels')

def extract_cards_checklists():
  print('fazer N - N para ter os cards e labels')


