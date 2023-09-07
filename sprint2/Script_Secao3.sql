--------------------------------------------------------------
--                  QUESTOES SECAO 3
--------------------------------------------------------------
--E1.Apresente a query para listar todos os livros publicados 
--após 2014. Ordenar pela coluna cod, em ordem crescente, as linhas.  
--Atenção às colunas esperadas no resultado final: cod, titulo, autor, 
--editora, valor, publicacao, edicao, idioma

select *
from livro
where publicacao >'2014-12-30'
order by cod asc

--------------------------------------------------------------
--------------------------------------------------------------
--E2.Apresente a query para listar os 10 livros mais caros.
--Ordenar as linhas pela coluna valor, em ordem decrescente. 
--Atenção às colunas esperadas no resultado final:  titulo, valor.

select DISTINCT titulo, valor 
from livro
order by valor desc
limit 10
--------------------------------------------------------------
--------------------------------------------------------------
--E3.Apresente a query para listar as 5 editoras com mais livros 
--na biblioteca. O resultado deve conter apenas as colunas quantidade, 
--nome, estado e cidade. Ordenar as linhas pela coluna que representa a 
--quantidade de livros em ordem decrescente.
--conta os tipos de editoras presentes
	
with editora_endereco as(
select editora.codeditora, endereco.estado,endereco.cidade
from editora
left join endereco
on editora.endereco = endereco.codendereco 
)

select count(publicacao) as quantidade, editora.nome, editora_endereco.estado, editora_endereco.cidade
from livro
left join editora
on livro.editora = editora.codeditora
left join editora_endereco
on livro.editora=editora_endereco.codeditora
group by editora.nome, editora_endereco.estado,editora_endereco.cidade
order by quantidade desc
limit 5	
--------------------------------------------------------------
--------------------------------------------------------------
--E4.Apresente a query para listar a quantidade de livros publicada por 
--cada autor. Ordenar as linhas pela coluna nome (autor), em ordem crescente.
--Além desta, apresentar as colunas codautor, nascimento e quantidade
--(total de livros de sua autoria)
		
select autor.nome, autor.codautor, autor.nascimento, count(livro.autor) as "quantidade"
from autor
left join livro
on autor.codautor = livro.autor
group by autor.nome, autor.codautor, autor.nascimento 
order by autor.nome asc

--------------------------------------------------------------
--------------------------------------------------------------
--E5.Apresente a query para listar o nome dos autores que publicaram livros
--através de editoras NÃO situadas na região sul do Brasil. Ordene o 
--resultado pela coluna nome, em ordem crescente.

with codeditora_estado as(
select editora.codeditora, endereco.estado
from editora
left join endereco
on editora.endereco = endereco.codendereco
)
select autor.nome
from livro
left join autor
on livro.autor = autor.codautor
left join codeditora_estado
on livro.editora = codeditora_estado.codeditora
where codeditora_estado.estado <> 'PARANÁ' or 'RIO GRANDE DO SUL' or 'SANTA CATARINA'
order by autor.nome asc

--------------------------------------------------------------
--------------------------------------------------------------
--E6.Apresente a query para listar o autor com maior número de livros
--publicados. O resultado deve conter apenas as colunas codautor, nome, 
--quantidade_publicacoes.

select autor.codautor, autor.nome,count(*) as quantidade_publicacoes
from livro
left join autor
on livro.autor = autor.codautor
group by autor.codautor
order by quantidade_publicacoes DESC 
limit 1	

--------------------------------------------------------------
--------------------------------------------------------------
--E7.Apresente a query para listar o nome dos autores com nenhuma publicação.
--Apresentá-los em ordem crescente
		
select autor.nome
from autor
left join livro
on autor.codautor = livro.autor
left join editora
on livro.editora = editora.codeditora
where publicacao is null
group by autor.nome
order by autor.nome

--------------------------------------------------------------
--------------------------------------------------------------
