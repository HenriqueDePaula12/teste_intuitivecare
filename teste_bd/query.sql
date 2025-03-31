-- Aqui temos ambas as querys análiticas realizadas utilizando CTEs

with operadoras_com_mais_despesas as (
	select 
		dc.reg_ans,
		SUM(vl_saldo_inicial - vl_saldo_final) AS total_despesa,
		dc.descricao,
		dc.data

	from 
		demonstracoes_contabeis as dc
	where 
		dc.descricao = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR '
		AND data >= '2024-10-01'  -- Início do 4º trimestre
        AND data <= '2024-12-31'   -- Fim do 4º trimestre
	group by 
        reg_ans, descricao, data
    order by
    	total_despesa desc
)
select
	reg_ans,
    total_despesa,
    descricao,
    data
FROM 
    operadoras_com_mais_despesas
limit 10

-- Segunda query

with operadoras_com_mais_despesas_ult_ano as (
	select 
		dc.reg_ans,
		SUM(vl_saldo_inicial - vl_saldo_final) AS total_despesa,
		dc.descricao,
		EXTRACT(YEAR FROM MAX(data)) AS ano

	from 
		demonstracoes_contabeis as dc
	where 
		dc.descricao = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR '
	group by 
        reg_ans, descricao
    order by
    	total_despesa desc
)
select
	reg_ans,
    total_despesa,
    descricao,
    ano
FROM 
    operadoras_com_mais_despesas_ult_ano
limit 10