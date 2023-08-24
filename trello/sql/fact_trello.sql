
WITH cte_bu AS(
    SELECT *,
        CASE  
            WHEN lb.nome IN ('Turnaround','Gestão de Turnaround') THEN 'TURNAROUND'
            WHEN lb.nome IN ('Controladoria') THEN 'CONTROLADORIA'
            WHEN lb.nome IN ('RJ') THEN 'RECUPERAÇÃO JUDICIAL'
            WHEN lb.nome IN ('BI','Safegold BI') THEN 'BI'
            WHEN lb.nome IN ('Industrial','Apoio Industrial') THEN 'INDUSTRIAL'
            ELSE 'BU NAO IDENTIFICADA'
        END AS unidade_negocio
    FROM labels lb 
    where lb.nome IN ('Turnaround','Gestão de Turnaround','Controladoria','RJ','BI','Safegold BI','Industrial','Apoio Industrial')
),
cte_status AS(
    SELECT *,
        CASE  
            WHEN lb.nome IN ('Não iniciado','Não Iniciada') THEN 'NÃO INICIADO'
            WHEN lb.nome IN ('Concluídos','Concluído','Pronto') THEN 'CONCLUÍDO'
            ELSE 'EM EXECUÇÃO'
        END AS status_card
    FROM labels lb 
    where lb.nome IN ('Não iniciado','Não Iniciada','Concluídos','Concluído','Pronto')
)
SELECT DISTINCT 
	b.id AS board_id,
	l.id AS list_id,
	c.id AS card_id,
	c.dt_inicio, 
	c.dt_entrega, 
	c.terminado,
  CASE
      WHEN bool_or(lb.nome IN ('Turnaround','Gestão de Turnaround','Controladoria','RJ','BI','Safegold BI','Industrial','Apoio Industrial')) THEN MAX(cte_bu.unidade_negocio)
      ELSE 'BU NAO IDENTIFICADA'
  END AS unidade_negocio,
  CASE
      WHEN bool_or(lb.nome IN ('Fora do Escopo','Escopo Adicional - Não Contratado','Fora do escopo')) THEN TRUE
      ELSE FALSE
  END AS fora_escopo,
  CASE
      WHEN bool_or(lb.nome IN ('Cliente')) THEN TRUE
      ELSE FALSE
  END AS acao_cliente,
  CASE
      WHEN bool_or(lb.nome IN ('Não iniciado','Não Iniciada','Concluídos','Concluído','Pronto')) THEN MAX(cte_status.status_card)
      ELSE 'EM EXECUÇÃO'
  END AS status_card
FROM cards c
	LEFT JOIN boards b ON c.id_board = b.id 
	LEFT JOIN lists l ON c.id_list = l.id 
	LEFT JOIN cards_labels cl ON cl.id_card = c.id 
	LEFT JOIN labels lb ON lb.id = cl.id_label 
	LEFT JOIN cte_bu ON lb.id = cte_bu.id
	LEFT JOIN cte_status ON lb.id = cte_status.id
GROUP BY 
    b.id, l.id, c.id, c.descricao, c.dt_inicio, c.dt_entrega, c.terminado