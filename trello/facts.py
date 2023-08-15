import os
import boto3
import csv
from dotenv import load_dotenv
import psycopg2 as pg 

load_dotenv()
# Conecta no redshift
redshift_engine = pg.connect(
    dbname=os.getenv("REDSHIFT_DBNAME"), user=os.getenv("REDSHIFT_USER"), host=os.getenv("REDSHIFT_HOST"), port=os.getenv("REDSHIFT_PORT"), password=os.getenv("REDSHIFT_PASSWORD")
)

def fact_cards(engine):
  script_path = os.path.join(os.path.dirname(__file__), 'sql/fact_cards.sql')

  with open(script_path, 'r', encoding='utf-8') as file:
      query = file.read()


  with engine.cursor() as cursor:
      cursor.execute('''
          CREATE TABLE IF NOT EXISTS fact_cards (
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
      cursor.execute('TRUNCATE TABLE fact_cards')
      engine.commit()
      # Executa o script SQL
      cursor.execute(query)

      # Itera sobre os resultados e executa a inserção
      for row in cursor.fetchall():
          insert_query_template = """
              INSERT INTO fact_cards (
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

      extract_data_to_csv(cursor)
      upload_csv_to_s3()
      copy_s3_to_redshift()
  cursor.close()
  engine.close()
  
def extract_data_to_csv(cursor):
    '''
    Extrai uma certa tabela do BD e transforma ela em um arquivo CSV
    '''
    source_table = 'fact_cards'

    # Obtém o caminho completo para o arquivo de saída
    csv_file_path = os.path.join(os.path.dirname(__file__), "csv", f'{source_table}.csv')

    # Remove o arquivo CSV se ele já existe
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # Escreve no arquivo CSV os valores da tabela
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)


        cursor.execute(f"SELECT * FROM {source_table}")
        column_names = [desc[0] for desc in cursor.description]
        csv_writer.writerow(column_names)

        for row in cursor:
            csv_writer.writerow(row)

# Function to upload the CSV file to Amazon S3
def upload_csv_to_s3():
  # Obtém o caminho completo para o diretório "csv"
  csv_directory = os.path.join(os.path.dirname(__file__), "csv")

  aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
  aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
  # Conecta-se ao serviço S3
  s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

  # Especifica o nome do bucket e o caminho para o arquivo no S3
  s3_bucket = os.getenv("REDSHIFT_BUCKET")
  s3_key = 'trello/fact_cards.csv'

  # Caminho local do arquivo CSV
  local_file_path = os.path.join(csv_directory, 'fact_cards.csv')

  # Faz o upload do arquivo para o S3
  s3.upload_file(local_file_path, s3_bucket, s3_key)

def copy_s3_to_redshift(): 
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