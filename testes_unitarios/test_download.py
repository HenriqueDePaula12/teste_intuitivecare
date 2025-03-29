import pytest
import pandas as pd

from unittest.mock import patch, MagicMock
from teste_bd.download import ProcessandoDados  # Certifique-se de que esse caminho está correto

@pytest.fixture
def processador():
    return ProcessandoDados()

@patch("teste_bd.download.requests.get")  # Corrigido caminho do módulo
def test_baixar_arquivos(mock_get, processador):
    mock_get.return_value.status_code = 200
    mock_get.return_value.iter_content = MagicMock(return_value=[b"data"])
    
    assert processador.baixar_arquivos() is True

@patch("teste_bd.download.zipfile.ZipFile")  # Corrigido caminho do módulo
@patch("teste_bd.download.os.listdir", return_value=["test.zip"])
@patch("teste_bd.download.os.remove")
def test_extrair_arquivos_zip(mock_remove, mock_listdir, mock_zip, processador):
    mock_zip.return_value.__enter__.return_value.extractall = MagicMock()
    assert processador.extrair_arquivos_zip() is True

@patch("teste_bd.download.pd.read_csv")
@patch("teste_bd.download.pd.DataFrame.to_csv")
@patch("teste_bd.download.os.listdir", return_value=["1T2024.csv"])
def test_tratar_demonstracoes_contabeis(mock_listdir, mock_to_csv, mock_read_csv, processador):
    # Criando um DataFrame realista para evitar problemas com MagicMock
    df_mock = pd.DataFrame({
    "DATA": ["01/01/2024", "02/01/2024"],
    "VL_SALDO_INICIAL": ["1000.50", "2500.75"],  # Sem ponto de milhar, com ponto decimal
    "VL_SALDO_FINAL": ["1200.25", "2800.90"],
    "REG_ANS": ["12345", "67890"],
    "CD_CONTA_CONTABIL": ["999", "888"]
})

    
    mock_read_csv.return_value = df_mock  # Retorna um DataFrame realista

    assert processador.tratar_demonstracoes_contabeis() is True

@patch("teste_bd.download.pd.read_csv")  # Corrigido caminho do módulo
@patch("teste_bd.download.pd.DataFrame.to_csv")
@patch("teste_bd.download.os.path.exists", return_value=True)
def test_tratar_relatorio_cadop(mock_exists, mock_to_csv, mock_read_csv, processador):
    df_mock = pd.DataFrame({
    "Registro_ANS": ["12345", "67890"],
    "CNPJ": ["12345678000195", "98765432000158"],
    "CEP": ["01001-000", "02002-000"],
    "DDD": ["11", "21"],
    "Telefone": ["1112345678", "2198765432"],
    "Fax": ["1132323232", "2198761234"],
    "Regiao_de_Comercializacao": ["SP", "RJ"],
    "Data_Registro_ANS": ["01/01/2020", "01/02/2021"]
})

    mock_read_csv.return_value = df_mock
