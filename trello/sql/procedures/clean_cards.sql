CREATE OR REPLACE PROCEDURE public.clean_cards()
 LANGUAGE sql
AS $procedure$

-- Clean labels
	delete from labels where nome = '' or  nome ilike '%Capa%';

-- Clean cards_labels
	delete from cards_labels 
	where id_label in (
		select id_label as label from cards_labels cl
		left join cards c on cl.id_card = c.id 
		left join labels l on l.id = cl.id_label 
		where l.nome like '%Capa%' or l.nome = ''
	);
	delete from cards_labels 
	where id_card  in (
		select id from cards where nome in ('Turnaround','BI','Controladoria','Cliente ')
	);

-- Clean cards
	delete from cards where nome in ('Turnaround','BI','Controladoria','Cliente '); 

$procedure$
;
