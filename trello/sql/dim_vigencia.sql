SELECT 
b.nome,
c.dt_inicio AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'  AS data_inicio, 
c.dt_entrega AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS data_entrega
FROM cards c 
LEFT JOIN boards b on b.id = c.id_board 
WHERE c.nome ILIKE '%Projeto: Escopo, Equipe%'