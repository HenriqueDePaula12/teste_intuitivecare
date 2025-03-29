CREATE TABLE demonstracoes_contabeis (
    id SERIAL PRIMARY KEY,
    DATA DATE NOT NULL,
    REG_ANS varchar(15),
    CD_CONTA_CONTABIL varchar(15),
    DESCRICAO text,
    VL_SALDO_INICIAL DECIMAL(15, 2),
    VL_SALDO_FINAL DECIMAL(15,2)
);

CREATE TABLE operadoras_de_plano_de_saude_ativas (
    id SERIAL PRIMARY KEY,
    Registro_ANS varchar(10),
    CNPJ varchar(14),
    Razao_Social text,
    Nome_Fantasia varchar(40),
    Modalidade varchar(30),
    Logradouro varchar(40),
    Numero varchar(6),
    Complemento varchar(50),
    Bairro varchar(20),
    Cidade varchar(30),
    UF varchar(2),
    CEP varchar(9),
    DDD varchar(3),
    Telefone varchar(9),
    Fax varchar(15),
    Endereco_eletronico varchar(50),
    Representante varchar(50),
    Cargo_Representante varchar(35),
    Regiao_de_Comercializacao varchar(5),
    Data_Registro_ANS DATE
)