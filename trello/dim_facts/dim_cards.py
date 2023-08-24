import os
from dotenv import load_dotenv
from . import redshift


def extract(engine):
  """
    Executa o script dim_cards.sql e insere ele uma tabela do PostgreSQL.
    Após isso chama os métodos `extract_data_to_csv(cursor,tabela)`, `upload_csv_to_s3(tabela)` e `copy_s3_to_redshift(tabela)` para inserir a tabela no Redshift.
      
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  load_dotenv()
  tabela = 'dim_cards'
  print('------------------------------------------')
  print(f'{tabela} -> Inicio da leitura do script')
  # Obtém o diretório atual em que o script está sendo executado
  current_directory = os.path.dirname(os.path.abspath(__file__))
  # Navega para o diretório 'sql' a partir do diretório atual
  script_path = os.path.abspath(os.path.join(current_directory, '..', f'sql/{tabela}.sql'))

  # Leitura do script SQL
  with open(script_path, 'r', encoding='utf-8') as file:
      query = file.read()

  # Criação da tabela
  with engine.cursor() as cursor:
      cursor.execute(f'''
          CREATE TABLE IF NOT EXISTS {tabela} (
            id TEXT NULL,
            nome TEXT NULL,
            descricao TEXT NULL,
            dt_inicio TIMESTAMP NULL,
            dt_entrega TIMESTAMP NULL,
            terminado BOOLEAN NULL,
            board_id TEXT NULL,
            list_id TEXT NULL
          );
      ''')
      cursor.execute(f'TRUNCATE TABLE {tabela}')
      engine.commit()
      # Executa o script SQL
      cursor.execute(query)

      # Itera sobre os resultados e executa a inserção na tabela
      for row in cursor.fetchall():
          insert_query_template = f"""
              INSERT INTO {tabela} (
                  id, nome, descricao, dt_inicio, dt_entrega, terminado, board_id, list_id
              ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s
              );
          """
          insert_values = (
              row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
          )
          cursor.execute(insert_query_template, insert_values)

      engine.commit()

      # Chama as funções necessárias para fazer a inserção dos dados no Redshift
      print(f'{tabela} -> Extração de dados SQL para um arquivo CSV')
      redshift.extract_data_to_csv(cursor,tabela)
      print(f'{tabela} -> Extração de dados CSV para um arquivo S3(AWS)')
      redshift.upload_csv_to_s3(tabela)
      print(f'{tabela} -> Copiando dados do arquivo S3 para a tabela do redshift')
      redshift.copy_s3_to_redshift(tabela)
  cursor.close()
