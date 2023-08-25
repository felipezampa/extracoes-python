import os
import boto3
import csv
import psycopg2 as pg 

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
  s3_key = f'trello/{tabela}.csv'


  # Faz o upload do arquivo para o S3
  s3.upload_file(csv_directory, s3_bucket, s3_key)

def copy_s3_to_redshift(tabela): 
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
    s3_key = f'trello/{tabela}.csv'
    
    # Criação da conexão com o Redshift
    redshift_conn = pg.connect(**redshift_params)
    
    # Criação do cursor
    cursor = redshift_conn.cursor()
    
    # Instrução COPY para carregar dados do S3 para o Redshift
    copy_query = f"""
    DELETE FROM trello_{tabela};
    COPY trello_{tabela}
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