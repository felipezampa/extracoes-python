import requests

def extract_fields(key,engine):
  print('------------------------------------------')
  print('extract_fields -> Inicio da leitura da URL')
  url = 'https://public-api2.ploomes.com/Fields'
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
    CREATE TABLE IF NOT EXISTS public.fields (
      id TEXT,
      key TEXT,
      dynamic TEXT,
      name TEXT,
      entity_id TEXT,
      secondary_entity_id TEXT,
      type_id TEXT,
      options_table_id TEXT,
      required TEXT,
      not_nullable TEXT,
      permanent TEXT,
      disabled TEXT,
      hidden TEXT,
      integration TEXT,
      filterable TEXT,
      wide TEXT,
      column_size TEXT,
      string_length TEXT
    );
  ''')
  cursor.execute('TRUNCATE TABLE fields')
  # Insere os dados na tabela
  print('extract_fields -> Inserção dos dados')
  # Atribui o array de objetos a variavel value_array
  value_array = data.get("value", [])
  # Loop em cada objeto dentro do array para insercao no banco
  for value in value_array:
    cursor.execute(
      '''
      INSERT INTO fields (
          id, key, dynamic, name, entity_id, secondary_entity_id, type_id, options_table_id,
          required, not_nullable, permanent, disabled, hidden, integration, filterable, wide,
          column_size, string_length
      ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      )
      ''', 
      ( 
          value["Id"], value["Key"], value["Dynamic"], value["Name"], value["EntityId"],
          value["SecondaryEntityId"], value["TypeId"], value["OptionsTableId"],
          value["Required"], value["NotNullable"], value["Permanent"], value["Disabled"],
          value["Hidden"], value["Integration"], value["Filterable"], value["Wide"],
          value["ColumnSize"], value["StringLength"]
      )
    )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print('extract_fields -> Extração concluida com sucesso!')