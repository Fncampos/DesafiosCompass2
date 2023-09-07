CREATE VIEW tb_cliente  AS
SELECT DISTINCT idCliente,
	nomeCliente, 
	cidadeCliente,
	estadoCliente,
	paisCliente
FROM tb_locacao
ORDER BY idCliente,
	nomeCliente,
	cidadeCliente, 
	estadoCliente, 
	paisCliente;
SELECT * FROM tb_cliente
 
-----------------------------------------

CREATE VIEW tb_carro AS
SELECT DISTINCT idCarro,
	marcaCarro,
	classiCarro,
	modeloCarro,
	anoCarro,
	idcombustivel as combustivel
FROM tb_locacao
ORDER BY idCarro,
	marcaCarro,
	classiCarro,
	modeloCarro,
	anoCarro,
	idcombustivel 
SELECT * FROM tb_carro

---------------------------------------
CREATE VIEW tb_combustivel AS
SELECT DISTINCT idcombustivel,
	tipoCombustivel
FROM tb_locacao

SELECT * FROM tb_combustivel

---------------------------------------

CREATE VIEW tb_vendedor AS
SELECT DISTINCT idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor 
FROM tb_locacao

SELECT * FROM tb_vendedor
---------------------------------------

CREATE VIEW tb2_locacao AS
SELECT idLocacao,
	SUBSTRING(dataLocacao, 1, 4) || '-' ||
    SUBSTRING(dataLocacao , 5, 2) || '-' ||
    SUBSTRING(dataLocacao , 7, 2) AS dtLocacao,
	horaLocacao,
	idVendedor,
	idCliente,
	idCarro,
	kmCarro,
	qtdDiaria,
	vlrDiaria,
	SUBSTRING(dataEntrega, 1, 4) || '-' ||
	SUBSTRING(dataEntrega, 5, 2) || '-' ||
	SUBSTRING(dataEntrega, 7, 2) AS dtEntrega,
	horaEntrega
FROM tb_locacao

SELECT * FROM tb2_locacao


