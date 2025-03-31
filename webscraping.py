import requests
import zipfile
import os

from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By

class WebScraper:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.pdf_urls = {}
        self.downloaded_files = []

    def accept_cookies(self):
        try:
            cookie_btn = self.driver.find_element(
                By.XPATH, "//button[@aria-label='Aceitar cookies']"
            )
            sleep(2)
            cookie_btn.click()
            sleep(2)
            return True
        except Exception as e:
            print(f"Erro ao aceitar cookies: {e}")
            return False

    def get_pdf_links(self):
        try:
            self.driver.get(
                "https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos"
            )
            self.driver.maximize_window()
            sleep(5)

            if not self.accept_cookies():
                print("Não foi possível aceitar os cookies")

            anexo1 = self.driver.find_element(
                By.XPATH, "//a[contains(text(), 'Anexo I.')]"
            )
            anexo2 = self.driver.find_element(
                By.XPATH, "//a[contains(text(), 'Anexo II.')]"
            )

            self.pdf_urls = {
                "Anexo_I.pdf": anexo1.get_attribute("href"),
                "Anexo_II.pdf": anexo2.get_attribute("href"),
            }
            return True
        except Exception as e:
            print(f"Erro ao obter links dos PDFs: {e}")
            return False
        finally:
            self.driver.quit()

    def download_pdfs(self):
    # Cria a pasta pdfs se não existir
        if not os.path.exists("pdfs"):
            os.makedirs("pdfs")
        
        for filename, url in self.pdf_urls.items():
            try:
                response = requests.get(url, stream=True)
                if response.status_code == 200:
                    filepath = os.path.join("pdfs", filename)
                    with open(filepath, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                    self.downloaded_files.append(filepath)
                    print(f"Download concluído: {filepath}")
                else:
                    print(
                        f"Erro ao baixar {filename}. Status code: {response.status_code}"
                    )
            except Exception as e:
                print(f"Falha ao baixar {filename}: {e}")

    def zip_files(self):
        if not self.downloaded_files:
            print("Nenhum arquivo foi baixado para compactar.")
            return False

        try:
            zip_filename = os.path.join("pdfs", "Anexos.zip")
            with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
                for file in self.downloaded_files:
                    zipf.write(file, os.path.basename(file))

            print(f"Arquivo ZIP criado: {zip_filename}")
            return True
        except Exception as e:
            print(f"Erro ao criar arquivo ZIP: {e}")
            return False

    def run(self):
        if self.get_pdf_links():
            self.download_pdfs()
            self.zip_files()


#if __name__ == "__main__":
#    scraper = WebScraper()
#    scraper.run()