import requests

def extract_lists(token,key,engine,idBoards):
  """
    Extrai informações de listas (lists) do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela='lists'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id text,
        nome text,
        fechado boolean,
        id_board text
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/lists?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 

    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      cursor.execute(
        f'''
            INSERT INTO {tabela} (id, nome, fechado, id_board)
            VALUES (%s, %s, %s, %s)
        ''', 
        (item['id'], item['name'], item['closed'], item['idBoard'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')
  