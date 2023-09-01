SELECT DISTINCT 
	c.id AS card_id,
	c.dt_inicio, 
	c.dt_entrega,
  	l.nome AS status_card
FROM cards c
	LEFT JOIN lists l ON l.id = c.id_list 
WHERE c.id_board = '620a6efd147ab42c8c3ce841'
GROUP BY 
   c.id, c.dt_inicio, c.dt_entrega, l.nome