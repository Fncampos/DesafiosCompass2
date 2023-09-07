-------------------------------------------------------------------------------------------------------
--                                    QUESTOES SECAO 4
-------------------------------------------------------------------------------------------------------
--E8.Apresente a query para listar o c�digo e o nome do vendedor com maior n�mero de vendas (contagem), 
--e que estas vendas estejam com o status conclu�da.  As colunas presentes no resultado devem ser, 
--portanto, cdvdd e nmvdd.

with maior_venda as (
	select cdvdd, status, count(status) as vendas
	from tbvendas
	group by cdvdd,status
	having status = 'Conclu�do'
	order by vendas desc
	limit 1
)
select maior_venda.cdvdd, tbvendedor.nmvdd
from maior_venda
left join tbvendedor
on maior_venda.cdvdd = tbvendedor.cdvdd

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E9.Apresente a query para listar o c�digo e nome do produto mais vendido entre as datas de 2014-02-03
--at� 2018-02-02, e que estas vendas estejam com o status conclu�da. As colunas presentes no resultado
--devem ser cdpro e nmpro.

select cdpro, nmpro
	from tbvendas
	where dtven >='2014-02-03' and dtven<= '2018-02-02' and status = 'Conclu�do'
	group by cdpro, nmpro
	order by count(cdpro) desc
	limit 1

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E10.A comiss�o de um vendedor � definida a partir de um percentual sobre o total de vendas 
--(quantidade * valor unit�rio) por ele realizado. O percentual de comiss�o de cada vendedor est�
--armazenado na coluna perccomissao, tabela tbvendedor. 
--Com base em tais informa��es, calcule a comiss�o de todos os vendedores, considerando todas as
--vendas armazenadas na base de dados com status conclu�do.
--As colunas presentes no resultado devem ser vendedor, valor_total_vendas e comissao. O valor
--de comiss�o deve ser apresentado em ordem decrescente arredondado na segunda casa decimal.

select
	tbvendedor.nmvdd as vendedor,
	round(sum(tbvendas.qtd*tbvendas.vrunt),2) as valor_total_vendas,
	round(sum(tbvendas.qtd*tbvendas.vrunt*tbvendedor.perccomissao*0.01),2) as comissao
				
from tbvendas
right join tbvendedor
on tbvendas.cdvdd = tbvendedor.cdvdd
where tbvendas.status = 'Conclu�do'
group by tbvendedor.nmvdd
order by comissao desc

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E11.Apresente a query para listar o c�digo e nome cliente com maior gasto na loja. As colunas presentes 
--no resultado devem ser cdcli, nmcli e gasto, esta �ltima representando o somat�rio das vendas
--(conclu�das) atribu�das ao cliente.

select cdcli, nmcli, round(sum(qtd*vrunt), 2) as gasto
from tbvendas
where status = 'Conclu�do'
group by cdcli, nmcli
order by gasto desc
limit 1

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E12.Apresente a query para listar c�digo, nome e data de nascimento dos dependentes do vendedor com
--menor valor total bruto em vendas (n�o sendo zero). As colunas presentes no resultado devem ser 
--cddep, nmdep, dtnasc e valor_total_vendas.
--Observa��o: Apenas vendas com status conclu�do.

select
	tbdependente.cddep,tbdependente.nmdep ,tbdependente.dtnasc, 
	round(sum(tbvendas.qtd*tbvendas.vrunt ),2) as valor_total_vendas
from tbvendas
left join tbdependente
on tbvendas.cdvdd = tbdependente.cdvdd
where tbvendas.status = 'Conclu�do'
group by tbvendas.cdvdd,tbdependente.cddep,tbdependente.nmdep ,tbdependente.dtnasc
order by valor_total_vendas ASC
limit 1

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E13.Apresente a query para listar os 10 produtos menos vendidos pelos canais de E-Commerce ou Matriz 
--(Considerar apenas vendas conclu�das).  As colunas presentes no resultado devem ser cdpro, 
--nmcanalvendas, nmpro e quantidade_vendas.

select cdpro,nmcanalvendas,nmpro, sum(qtd) as quantidade_vendas
from tbvendas
where status = 'Conclu�do' and (cdcanalvendas = 1 or cdcanalvendas =2)
group by cdpro,nmcanalvendas, nmpro
order by  quantidade_vendas asc
limit 10

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E14.Apresente a query para listar o gasto m�dio por estado da federa��o. As colunas presentes no 
--resultado devem ser estado e gastomedio. Considere apresentar a coluna gastomedio arredondada na 
--segunda casa decimal e ordenado de forma decrescente. Observa��o: Apenas vendas com status conclu�do.

select estado, round((avg(qtd*vrunt)),2) as gastomedio
from tbvendas
where status = 'Conclu�do'
group by estado
order by gastomedio desc

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E15.Apresente a query para listar os c�digos das vendas identificadas como deletadas. Apresente o
--resultado em ordem crescente.

select cdven
from tbvendas
where deletado = '1'
order by cdven asc

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--E16. Apresente a query para listar a quantidade m�dia vendida de cada produto agrupado por estado da 
--federa��o. As colunas presentes no resultado devem ser estado e nmprod e quantidade_media. Considere 
--arredondar o valor da coluna quantidade_media na quarta casa decimal. Ordene os resultados pelo estado
-- (1�) e nome do produto (2�). Obs: Somente vendas conclu�das.

select estado,nmpro, round((avg(qtd)),4) as quantidade_media
from tbvendas
where status = 'Conclu�do'
group by nmpro, estado
order by estado asc, nmpro asc

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------


