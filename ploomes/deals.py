import requests

def extract_deals(key,engine):
  print('------------------------------------------')
  print('extract_deals -> Inicio da leitura da URL')
  url = 'https://public-api2.ploomes.com/Deals'
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
    CREATE TABLE IF NOT EXISTS public.deals (
        id TEXT,
        title TEXT,
        contact_id TEXT,
        contact_name TEXT,
        stage_id TEXT,
        status_id TEXT,
        has_scheduled_tasks TEXT,
        tasks_ordination TEXT,
        last_quote_id TEXT,
        is_last_quote_approved TEXT,
        owner_id TEXT,
        start_date TEXT,
        currency_id TEXT,
        amount TEXT,
        start_currency_id TEXT,
        start_amount TEXT,
        last_interaction_record_id TEXT,
        days_in_stage TEXT,
        hours_in_stage TEXT,
        length TEXT,
        creator_id TEXT,
        updater_id TEXT,
        create_date TEXT,
        last_update_date TEXT
    );
  ''')
  cursor.execute('TRUNCATE TABLE deals')
  # Insere os dados na tabela
  print('extract_deals -> Inserção dos dados')
  # Atribui o array de objetos a variavel value_array
  value_array = data.get("value", [])
  # Loop em cada objeto dentro do array para insercao no banco
  for deal_data in value_array:
    cursor.execute(
      '''
      INSERT INTO deals (
          id, title, contact_id, contact_name, stage_id, status_id, has_scheduled_tasks,
          tasks_ordination, last_quote_id, is_last_quote_approved, owner_id, start_date,
          currency_id, amount, start_currency_id, start_amount, last_interaction_record_id,
          days_in_stage, hours_in_stage, length, creator_id, updater_id,
          create_date, last_update_date
      ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      )
      ''', 
      ( 
          deal_data["Id"], deal_data["Title"], deal_data["ContactId"],
          deal_data["ContactName"], deal_data["StageId"], deal_data["StatusId"],
          deal_data["HasScheduledTasks"], deal_data["TasksOrdination"],
          deal_data["LastQuoteId"], deal_data["IsLastQuoteApproved"],
          deal_data["OwnerId"], deal_data["StartDate"], deal_data["CurrencyId"],
          deal_data["Amount"], deal_data["StartCurrencyId"], deal_data["StartAmount"],
          deal_data["LastInteractionRecordId"], deal_data["DaysInStage"], 
          deal_data["HoursInStage"], deal_data["Length"], deal_data["CreatorId"], 
          deal_data["UpdaterId"], deal_data["CreateDate"], deal_data["LastUpdateDate"]
      )
    )

  # Confirma as alterações no banco de dados
  engine.commit()
  cursor.close()
  print('extract_deals -> Extração concluida com sucesso!')