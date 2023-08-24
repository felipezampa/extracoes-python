SELECT 
	b.id AS board_id,
	c2.id AS card_id,
	m.nome AS usuario,
	c.nome AS checklist, 
	left(ci.nome, 200) AS item,
  ci.dt_entrega, 
	CASE 
		WHEN ci.estado = 'complete' THEN TRUE
		ELSE FALSE
	END AS terminado
FROM checklists_items ci 
	LEFT JOIN checklists c ON c.id = ci.id_checklist 
	LEFT JOIN boards b ON c.id_board = b.id 
	LEFT JOIN cards c2 ON c2.id = c.id_card 
	LEFT JOIN members m ON m.id = ci.id_membro 
WHERE b.fechado IS FALSE
ORDER BY b.nome