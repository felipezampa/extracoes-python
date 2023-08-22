import requests

def extract_labels(token,key,engine,idBoards):
  """
    Extrai informações de etiquetas (labels) do Trello e
    insere essas informações em uma tabela do PostgreSQL.

    `token: Token de autenticação do Trello.`
    `key: Chave de API do Trello.`
    `engine: Conexão ao banco de dados PostgreSQL.`
    `idBoards: Lista de IDs dos quadros do Trello.`
  """
  tabela='labels'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura da URL')
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute(f'''
      CREATE TABLE IF NOT EXISTS public.{tabela} (
        id_board text,
        id text,
        nome text,
        cor text,
        quantidade_utilizada INTEGER
      );
  ''')
  cursor.execute(f'TRUNCATE TABLE {tabela}')

  # Loop para fazer as requests de vários boards, o id é o objeto e o index é a posição no array
  for index, id in enumerate(idBoards):
    url = 'https://api.trello.com/1/boards/{}/labels?key={}&token={}'.format(id, key, token)

    # Recupera as informações da url
    response = requests.get(url)
    # Converte a resposta para um objeto JSON
    data = response.json() 
    # Cria um cursor para executar as consultas SQL
    
    # Insere os dados na tabela
    print(f'{tabela} -> idBoard: ' + str(index) + ' - Inserção dos dados')
    for item in data:
      cursor.execute(
        f'''
            INSERT INTO {tabela} (id_board, id, nome, cor, quantidade_utilizada)
            VALUES (%s, %s, %s, %s, %s)
        ''', 
        (item['idBoard'], item['id'], item['name'], item['color'], item['uses'])
      )

  # Confirma as alterações no banco de dados e fecha o cursor
  engine.commit()
  cursor.close()
  print(f'{tabela} -> Extração concluída com sucesso!')
  