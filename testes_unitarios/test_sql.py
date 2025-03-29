import pytest
from unittest.mock import patch, MagicMock
import os
from dotenv import load_dotenv
from teste_bd.scripts_sql import PostgresDataImporter

# Mock para garantir que o método de conexão ao banco seja simulado
@pytest.fixture
def processador():
    return PostgresDataImporter()

# Teste de conexão com o banco de dados
@patch("teste_bd.scripts_sql.psycopg2.connect")
@patch("teste_bd.scripts_sql.os.getenv")
def test_conectar_banco(mock_getenv, mock_connect, processador):
    # Simulando as variáveis de ambiente
    mock_getenv.return_value = "fakevalue"
    mock_connect.return_value = MagicMock()  # Simula a conexão com o banco

    conn = processador.conectar_banco()

    print(f"Conexão retornada: {conn}")

    assert conn is not None
    mock_connect.assert_called_once_with(
        host="fakevalue", 
        database="fakevalue", 
        user="fakevalue", 
        password="fakevalue", 
        port=5432
    )

# Teste da criação de tabelas no banco de dados
@patch("teste_bd.scripts_sql.psycopg2.connect")
def test_criar_tabelas(mock_connect, processador):
    conn = MagicMock()
    cursor_mock = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_mock
    mock_connect.return_value = conn

    # Chama o método
    processador.criar_tabelas(conn)

    # Pega TODAS as chamadas do execute
    execute_calls = [call_args[0][0].strip().lower().replace('\n', '').replace(' ', '') for call_args in cursor_mock.execute.call_args_list]

    # Define as queries esperadas com mesmo formato
    expected_query1 = """
        CREATE TABLE IF NOT EXISTS demonstracoes_contabeis (
            id SERIAL PRIMARY KEY,
            DATA DATE NOT NULL,
            REG_ANS varchar(15),
            CD_CONTA_CONTABIL varchar(15),
            DESCRICAO text,
            VL_SALDO_INICIAL DECIMAL(15, 2),
            VL_SALDO_FINAL DECIMAL(15,2)
        );
    """.strip().lower().replace('\n', '').replace(' ', '')

    expected_query2 = """
        CREATE TABLE IF NOT EXISTS operadoras_de_plano_de_saude_ativas (
            id SERIAL PRIMARY KEY,
            Registro_ANS varchar(10),
            CNPJ varchar(14),
            Razao_Social text,
            Nome_Fantasia varchar(110),
            Modalidade varchar(50),
            Logradouro varchar(40),
            Numero varchar(15),
            Complemento varchar(50),
            Bairro varchar(50),
            Cidade varchar(30),
            UF varchar(2),
            CEP varchar(9),
            DDD varchar(3),
            Telefone varchar(30),
            Fax varchar(15),
            Endereco_eletronico varchar(50),
            Representante varchar(50),
            Cargo_Representante varchar(110),
            Regiao_de_Comercializacao varchar(5),
            Data_Registro_ANS DATE
        );
    """.strip().lower().replace('\n', '').replace(' ', '')

    assert expected_query1 in execute_calls
    assert expected_query2 in execute_calls
    conn.commit.assert_called_once()

# Teste da importação de arquivos CSV
@patch("teste_bd.scripts_sql.psycopg2.connect")
@patch("teste_bd.scripts_sql.os.path.exists", return_value=True)
@patch("teste_bd.scripts_sql.open", create=True)
def test_importar_csv(mock_open, mock_exists, mock_connect, processador):
    # Setup da conexão
    conn = MagicMock()
    cursor_mock = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_mock
    mock_connect.return_value = conn

    # Setup do open e exists
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    mock_exists.return_value = True  # Simula que o arquivo existe

    # Caminho do CSV usado
    csv_path = processador.CSV_FILES[0]

    # Chama o método a ser testado
    processador.importar_csv(conn, csv_path)

    # Normaliza a query esperada
    copy_sql_expected = """
        COPY demonstracoes_contabeis(DATA, REG_ANS, CD_CONTA_CONTABIL, DESCRICAO, VL_SALDO_INICIAL, VL_SALDO_FINAL)
        FROM STDIN WITH (FORMAT CSV, DELIMITER ';', HEADER)
    """.strip().lower().replace('\n', '').replace(' ', '')

    # Captura o argumento real passado no mock
    called_args = cursor_mock.copy_expert.call_args[0]
    called_sql = called_args[0].strip().lower().replace('\n', '').replace(' ', '')

    # Valida que a query foi executada corretamente
    assert copy_sql_expected == called_sql
    assert called_args[1] == mock_file

    # Garante que o commit foi chamado
    conn.commit.assert_called_once()

# Teste da importação do arquivo de relatório CSV
@patch("teste_bd.scripts_sql.psycopg2.connect")
@patch("teste_bd.scripts_sql.os.path.exists", return_value=True)
@patch("teste_bd.scripts_sql.open", create=True)
def test_importar_relatorio_csv(mock_open, mock_exists, mock_connect, processador):
    # Setup da conexão
    conn = MagicMock()
    cursor_mock = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_mock
    mock_connect.return_value = conn

    # Simula o arquivo do relatório CSV
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file

    # Executa o método a ser testado
    processador.importar_relatorio_csv(conn)

    # Normaliza a SQL esperada
    expected_sql = """
        COPY operadoras_de_plano_de_saude_ativas(
            Registro_ANS, CNPJ, Razao_Social, Nome_Fantasia, Modalidade,
            Logradouro, Numero, Complemento, Bairro, Cidade, UF, CEP, DDD,
            Telefone, Fax, Endereco_eletronico, Representante, Cargo_Representante,
            Regiao_de_Comercializacao, Data_Registro_ANS
        )
        FROM STDIN WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '\\', HEADER)
    """.strip().lower().replace('\n', '').replace(' ', '')

    # Captura a chamada feita no mock
    called_args = cursor_mock.copy_expert.call_args[0]
    called_sql = called_args[0].strip().lower().replace('\n', '').replace(' ', '')

    # Faz asserções
    assert expected_sql == called_sql
    assert called_args[1] == mock_file
    conn.commit.assert_called_once()


## Teste do método run que chama os outros métodos
#@patch("teste_bd.scripts_sql.psycopg2.connect")
#@patch("teste_bd.scripts_sql.os.path.exists", return_value=True)
#@patch("teste_bd.scripts_sql.open", create=True)
#def test_run(mock_open, mock_exists, mock_connect, processador):
#    conn = MagicMock()
#    mock_connect.return_value = conn
#    mock_file = MagicMock()
#    mock_open.return_value.__enter__.return_value = mock_file
#
#    # Testa o método run
#    processador.run()
#
#    # Verifica se os métodos foram chamados na ordem correta
#    processador.criar_tabelas.assert_called_once_with(conn)
#    processador.importar_csv.assert_any_call(conn, processador.CSV_FILES[0])
#    processador.importar_relatorio_csv.assert_called_once_with(conn)
#    conn.close.assert_called_once()
#