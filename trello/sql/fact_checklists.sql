SELECT 
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
	LEFT JOIN cards c2 ON c2.id = c.id_card 
	LEFT JOIN members m ON m.id = ci.id_membro 
WHERE c2.id IS NOT NULL