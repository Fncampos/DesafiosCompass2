CREATE VIEW fato_locacao  AS
SELECT idLocacao AS idData,
	idLocacao AS idData,
	idCarro,
	idVendedor,
	idCliente,
	kmCarro,
	qtdDiaria,
	vlrDiaria
FROM tb_locacao;	

SELECT * FROM fato_locacao


------------------------------------------------------------------------------


CREATE VIEW dim_data  AS
SELECT idLocacao AS idData,
	SUBSTRING(dataLocacao, 1, 4) || '-' ||
    SUBSTRING(dataLocacao , 5, 2) || '-' ||
    SUBSTRING(dataLocacao , 7, 2) AS dtLocacao,
	horaLocacao,
	SUBSTRING(dataLocacao, 5, 2) AS mesLocacao,
	SUBSTRING(dataLocacao, 1, 4) AS anoLocacao,
	SUBSTRING(dataEntrega, 1, 4) || '-' ||
	SUBSTRING(dataEntrega, 5, 2) || '-' ||
	SUBSTRING(dataEntrega, 7, 2) AS dtEntrega,
	horaEntrega,
	SUBSTRING(dataEntrega, 5, 2) AS mesEntrega,
	SUBSTRING(dataEntrega, 1, 4) AS anoEntrega
	FROM tb_locacao;

SELECT * FROM dim_data


------------------------------------------------------------------------------


CREATE VIEW dim_cliente  AS
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

SELECT * FROM dim_cliente

------------------------------------------------------------------------------

CREATE VIEW dim_carro AS
SELECT DISTINCT idCarro,
	marcaCarro,
	classiCarro,
	modeloCarro,
	anoCarro,
	tipoCombustivel
FROM tb_locacao
ORDER BY idCarro,
	marcaCarro,
	classiCarro,
	modeloCarro,
	anoCarro,
	idcombustivel
	
SELECT * FROM dim_carro

------------------------------------------------------------------------------

CREATE VIEW dim_vendedor AS
SELECT DISTINCT idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor
FROM tb_locacao

SELECT * FROM dim_vendedor

-------------------------------------------------------------------------------


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
