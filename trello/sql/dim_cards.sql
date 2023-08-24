SELECT 
	id, 
  left(nome, 200) AS nome,  
	left(descricao,200) AS descricao,
	dt_inicio, 
	dt_entrega, 
	terminado, 
	id_board AS board_id, 
	id_list AS list_id 
FROM cards 