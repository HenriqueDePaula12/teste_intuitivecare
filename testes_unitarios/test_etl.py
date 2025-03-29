import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from etl import PDFProcessor  # Supondo que o código esteja no arquivo processor.py

@pytest.fixture
def processor():
    return PDFProcessor()

def test_extrair_dados_pdf(processor):
    mock_table = [["Col1", "Col2"], ["Dado1", "Dado2"], ["Dado3", "Dado4"]]
    mock_page = MagicMock()
    mock_page.extract_table.return_value = mock_table
    
    with patch("pdfplumber.open") as mock_pdf:
        mock_pdf.return_value.__enter__.return_value.pages = [None, None, mock_page]
        
        result = processor.extrair_dados_pdf()
        
        assert result is True
        assert processor.df is not None
        assert list(processor.df.columns) == ["Col1", "Col2"]
        assert len(processor.df) == 2  # Apenas os dados, sem cabeçalho

def test_processar_dados(processor):
    data = {
        "RN\n(alteração)": ["A1", "A2"],
        "VIGÊNCIA": ["2023", "2024"],
        "CAPÍTULO": ["X", "Y"],
        "OD": ["Sim", "Não"],
        "AMB": ["Não", "Sim"]
    }
    processor.df = pd.DataFrame(data)
    processor.processar_dados()
    
    expected_columns = ["RN_alteracao", "VIGENCIA", "CAPITULO", "Seg_Odontologica", "Seg_Ambulatorial"]
    assert list(processor.df.columns) == expected_columns

def test_gerar_zip(processor):
    with patch("zipfile.ZipFile") as mock_zip:
        processor.gerar_zip()
        mock_zip.assert_called_once()

def test_run(processor):
    processor.extrair_dados_pdf = MagicMock(return_value=True)
    processor.processar_dados = MagicMock()
    processor.gerar_zip = MagicMock()
    
    processor.run()
    processor.extrair_dados_pdf.assert_called_once()
    processor.processar_dados.assert_called_once()
    processor.gerar_zip.assert_called_once()
