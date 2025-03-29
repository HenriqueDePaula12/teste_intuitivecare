import pytest
from unittest.mock import MagicMock, patch
from webscraping import WebScraper  # Supondo que o código esteja no arquivo scraper.py

@pytest.fixture
def scraper():
    return WebScraper()

def test_accept_cookies(scraper):
    scraper.driver.find_element = MagicMock()
    scraper.accept_cookies()
    scraper.driver.find_element.assert_called_once()

def test_get_pdf_links(scraper):
    scraper.driver.get = MagicMock()
    scraper.driver.maximize_window = MagicMock()
    scraper.accept_cookies = MagicMock(return_value=True)
    scraper.driver.find_element = MagicMock(side_effect=[MagicMock(), MagicMock()])
    
    result = scraper.get_pdf_links()
    assert result is True
    assert len(scraper.pdf_urls) == 2

def test_download_pdfs(scraper):
    scraper.pdf_urls = {
        "Anexo_I.pdf": "https://example.com/anexo1.pdf",
        "Anexo_II.pdf": "https://example.com/anexo2.pdf",
    }
    
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content = MagicMock(return_value=[b"data"])
        mock_get.return_value = mock_response
        
        scraper.download_pdfs()
        assert len(scraper.downloaded_files) == 2

def test_zip_files(scraper):
    with patch("zipfile.ZipFile") as mock_zip:
        scraper.downloaded_files = ["pdfs/Anexo_I.pdf", "pdfs/Anexo_II.pdf"]
        result = scraper.zip_files()
        assert result is True
        mock_zip.assert_called_once()

def test_run(scraper):
    scraper.get_pdf_links = MagicMock(return_value=True)
    scraper.download_pdfs = MagicMock()
    scraper.zip_files = MagicMock()
    scraper.run()
    scraper.get_pdf_links.assert_called_once()
    scraper.download_pdfs.assert_called_once()
    scraper.zip_files.assert_called_once()
