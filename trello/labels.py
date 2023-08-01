import requests

def extract_labels(token,key,engine,idBoards):
  print('------------------------------------------')
  print('extract_labels -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS public.labels (
        id_board text,
        id text,
        nome text,
        cor text,
        quantidade_utilizada INTEGER
      );
  ''')
  cursor.execute('TRUNCATE TABLE labels')
  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/labels?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print('extract_labels -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      cursor.execute(
        '''
            INSERT INTO labels (id_board, id, nome, cor, quantidade_utilizada)
            VALUES (%s, %s, %s, %s, %s)
        ''', 
        (item['idBoard'], item['id'], item['name'], item['color'], item['uses'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print('extract_labels -> Extração concluída com sucesso!')
  