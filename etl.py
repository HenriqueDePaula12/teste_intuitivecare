import pdfplumber
import pandas as pd
import zipfile

class PDFProcessor:
    def __init__(self):
        self.pdf_path = "pdfs/Anexo_I.pdf"
        self.output_csv = "Anexo_I.csv"
        self.cabecalho = None
        self.dados_completos = []
        self.df = None

    def extrair_dados_pdf(self):
        with pdfplumber.open(self.pdf_path) as pdf:
            for i, pagina in enumerate(pdf.pages[2:]):
                tabela = pagina.extract_table()
                if tabela:
                    if i == 0:
                        self.cabecalho = tabela[0]
                        self.dados_completos.extend(tabela[1:])
                    else:
                        self.dados_completos.extend(tabela[1:])
        
        if self.cabecalho and self.dados_completos:
            self.df = pd.DataFrame(self.dados_completos, columns=self.cabecalho)
            return True
        return False

    def processar_dados(self):
        self.df = self.df.rename(columns={
            'RN\n(alteração)': 'RN_alteracao',
            'VIGÊNCIA': 'VIGENCIA',
            'CAPÍTULO': 'CAPITULO',
            'OD': 'Seg_Odontologica',
            'AMB': 'Seg_Ambulatorial'
        })
        self.df.to_csv(self.output_csv, index=False, encoding="utf-8-sig")

    def gerar_zip(self):
        with zipfile.ZipFile('Teste_Henrique.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(self.output_csv)

    def run(self):
        if self.extrair_dados_pdf():
            self.processar_dados()
            self.gerar_zip()

# Como usar:
#if __name__ == "__main__":
#    processor = PDFProcessor()
#    
#    if processor.extrair_dados_pdf():
#        processor.renomear_colunas()
#        processor.salvar_csv()
#        processor.zipar_arquivo()
#    else:
#        print("Falha ao extrair dados do PDF")