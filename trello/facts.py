import os
import pandas as pd
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
      pg_to_redshift(redshift_engine, engine)
  engine.close()




# def table_to_csv(engine):

  with engine.cursor() as cursor:
      # Executa a consulta SQL para buscar os dados da tabela
      query = "SELECT * FROM fact_cards;"
      cursor.execute(query)

      # Recupera os dados da consulta
      data = cursor.fetchall()

  # Cria um DataFrame do pandas com os dados
  columns = [
      "board_id", "board", "list", "card_id", "card",
      "descricao", "dt_inicio", "dt_entrega", "terminado",
      "unidade_negocio", "fora_escopo", "acao_cliente", "status_card"
  ]
  df = pd.DataFrame(data, columns=columns)

  # Obtém o caminho completo para o arquivo de saída
  output_path = os.path.join(os.path.dirname(__file__), "csv", 'fact_cards.csv')

  # Remove o arquivo CSV se ele já existe
  if os.path.exists(output_path):
      os.remove(output_path)

  # Cria o diretório se ele não existir
  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  # Exporta o DataFrame para um arquivo CSV
  df.to_csv(output_path, index=False, encoding='utf-8',sep='"')

# def csv_to_s3:

def pg_to_redshift(redshift_engine, postgres_engine): 
    postgres_cursor = postgres_engine.cursor()
    redshift_cursor = redshift_engine.cursor()

    postgres_cursor.execute('SELECT * FROM fact_cards')
    result = postgres_cursor.fetchall()

    # Define o comando INSERT para o Redshift
    insert_query = """
        INSERT INTO fact_trello (
            board_id, board, list, card_id, card,
            descricao, dt_inicio, dt_entrega, terminado,
            unidade_negocio, fora_escopo, acao_cliente, status_card
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    # Itera sobre os resultados e executa a inserção no Redshift
    for row in result:
        try:
            redshift_cursor.execute(insert_query, row)
        except Exception as e:
            print("Erro ao inserir linha:")
            print(row)
            print("Erro:", e)

    # Confirma as transações no Redshift
    redshift_engine.commit()

    redshift_cursor.close()

