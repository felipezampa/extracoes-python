import requests

def extract_users(key,engine):
  print('------------------------------------------')
  print('extract_users -> Inicio da leitura da URL')
  url = 'https://public-api2.ploomes.com/Users'
  # Inclui a key passada como parametro do arquivo .env no header da requisicao
  request_headers = {
        "User-Key": key
  }
  # Recupera as informações da url
  response = requests.get(url,headers=request_headers)
  # Converte a resposta para um objeto JSON
  data = response.json()  
  # Cria um cursor para executar as consultas SQL
  cursor = engine.cursor()

  # Cria a tabela se ela ainda não existir
  cursor.execute('''
    CREATE TABLE IF NOT EXISTS public.users (
        id TEXT,
        name TEXT,
        email TEXT,
        avatar_url TEXT,
        profile_id TEXT
    );
  ''')
  cursor.execute('TRUNCATE TABLE users')
  # Insere os dados na tabela
  print('extract_users -> Inserção dos dados')
  # Atribui o array de objetos a variavel value_array
  value_array = data.get("value", [])
  # Loop em cada objeto dentro do array para insercao no banco
  # print(value_array[0])
  for value in value_array:
    cursor.execute(
      '''
      INSERT INTO users (
          id, name, email, avatar_url, profile_id
      ) VALUES (
          %s, %s, %s, %s, %s
      )
      ''', 
      ( 
          value["Id"], value["Name"], value["Email"], value["AvatarUrl"], value["ProfileId"]
      )
    )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print('extract_users -> Extração concluida com sucesso!')