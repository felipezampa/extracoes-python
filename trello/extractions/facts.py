import os
import boto3
import csv
from dotenv import load_dotenv
import psycopg2 as pg 


def fact_cards(engine):
  """
    Executa o script fact_cards.sql e insere ele uma tabela do PostgreSQL.
    Após isso chama os métodos `extract_data_to_csv(cursor)`, `upload_csv_to_s3()` e `copy_s3_to_redshift()` para inserir a tabela no Redshift.
      
    `engine: Conexão ao banco de dados PostgreSQL.`
  """
  load_dotenv()
  tabela = 'fact_cards'
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
              board_id TEXT,
              board TEXT,
              list TEXT,
              card_id TEXT,
              card TEXT,
              descricao TEXT,
              dt_inicio TIMESTAMP,
              dt_entrega TIMESTAMP,
              terminado BOOLEAN,
              unidade_negocio TEXT,
              fora_escopo BOOLEAN,
              acao_cliente BOOLEAN,
              status_card TEXT
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
                  board_id, board, list, card_id, card,
                  descricao, dt_inicio, dt_entrega, terminado,
                  unidade_negocio, fora_escopo, acao_cliente, status_card
              ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, 
                  %s, %s, %s, %s, %s, %s
              );
          """
          insert_values = (
              row[0], row[1], row[2], row[3], row[4],
              row[5], row[6], row[7], row[8],
              row[9], row[10], row[11], row[12]
          )
          cursor.execute(insert_query_template, insert_values)

      engine.commit()

      # Chama as funções necessárias para fazer a inserção dos dados no Redshift
      print(f'{tabela} -> Extração de dados SQL para um arquivo CSV')
      extract_data_to_csv(cursor,tabela)
      print(f'{tabela} -> Extração de dados CSV para um arquivo S3(AWS)')
      upload_csv_to_s3(tabela)
      print(f'{tabela} -> Copiando dados do arquivo S3 para a tabela do redshift')
      copy_s3_to_redshift()
  cursor.close()
  
def extract_data_to_csv(cursor,tabela):
    '''
    Extrai uma tabela do BD e transforma ela em um arquivo CSV
    '''
    
    # Obtém o diretório atual em que o script está sendo executado
    current_directory = os.path.dirname(os.path.abspath(__file__))
    # Navega para o diretório csv a partir do diretório atual
    csv_file_path = os.path.abspath(os.path.join(current_directory, '..', f'csv/{tabela}.csv'))

    # Remove o arquivo CSV se ele já existe
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # Escreve no arquivo CSV os valores da tabela
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        cursor.execute(f"SELECT * FROM {tabela}")
        column_names = [desc[0] for desc in cursor.description]
        csv_writer.writerow(column_names)

        for row in cursor:
            csv_writer.writerow(row)

def upload_csv_to_s3(tabela):
  '''
    Faz o upload de um arquivo CSV para o Amazon S3
  '''
  # Obtém o diretório atual em que o script está sendo executado
  current_directory = os.path.dirname(os.path.abspath(__file__))
  # Navega para o diretório csv a partir do diretório atual
  csv_directory = os.path.abspath(os.path.join(current_directory, '..', f'csv/{tabela}.csv'))
  # Chaves de acesso ao AWS
  aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
  aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
  # Conecta-se ao serviço S3
  s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

  # Especifica o nome do bucket e o caminho para o arquivo no S3
  s3_bucket = os.getenv("REDSHIFT_BUCKET")
  s3_key = 'trello/fact_cards.csv'


  # Faz o upload do arquivo para o S3
  s3.upload_file(csv_directory, s3_bucket, s3_key)

def copy_s3_to_redshift(): 
    '''
      Copia um arquivo s3 para uma tabela do Redshift
    '''
    # Parâmetros de conexão com o Redshift
    redshift_params = {
        "dbname": os.getenv("REDSHIFT_DBNAME"),
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "host": os.getenv("REDSHIFT_HOST"),
        "port": os.getenv("REDSHIFT_PORT")
    }

    # Parâmetros do S3
    s3_bucket = os.getenv("REDSHIFT_BUCKET")
    s3_key = 'trello/fact_cards.csv'

    # Nome da tabela no Redshift
    redshift_table = "fact_trello"
    
    # Criação da conexão com o Redshift
    redshift_conn = pg.connect(**redshift_params)
    
    # Criação do cursor
    cursor = redshift_conn.cursor()
    
    # Instrução COPY para carregar dados do S3 para o Redshift
    copy_query = f"""
    COPY {redshift_table}
    FROM 's3://{s3_bucket}/{s3_key}'
    CREDENTIALS 'aws_access_key_id={os.getenv("AWS_ACCESS_KEY_ID")};aws_secret_access_key={os.getenv("AWS_SECRET_ACCESS_KEY")}'
    CSV DELIMITER ';'
    IGNOREHEADER 1;
    """
    
    # Executa a instrução COPY
    cursor.execute(copy_query)
    
    # Commit e fechamento
    redshift_conn.commit()
    cursor.close()
    redshift_conn.close()