# Intuitive Care

Este é um projeto que visa realizar o teste de nivelamento da Intuitive Care


## Criação ambiente Python

Segue os comandos utilizados para criação do ambiente python

```bash
# Linux
python3 -m venv nome_do_ambiente
source nome_do_ambiente/bin/activate

#Windows
python3 -m venv nome_do_ambiente
nome_do_ambiente\Scripts\activate

# Após entrar no ambiente python, baixe os requirements para usar as libs do projeto

pip install -r requirements.txt
```



# WebScraping

#### [webscraping.py](intuitivecare/webscraping.py)

Este é nosso código python que realiza o webscraping usando selenium para download dos 2 anexos solicitados e depois coloca ambos os anexos em zip.

```python
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
```

# ETL

#### [etl.py](intuitivecare/etl.py)

Aqui é o segundo step do teste onde extraimos as tabelas do PDF Anexo I, salvamos em uma tabela formato csv, substituindo os nomes das colunas solicitadas pela descrição completa e compacta o arquivo csv em um zip com o nome requisitado.

```python
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
```

# Download

#### [download.py](intuitivecare/teste_bd/download.py)

Neste script seguimos o step 3 do teste onde baixamos uma lista de arquivos pedidos, após isso extraimos todos os arquivos do primeiro link que todos eram zips, após extraidos eles são csvs e então realizamos alguns tratamentos em todos os arquivos.

```python
import requests
import zipfile
import os
import pandas as pd
import re

class ProcessandoDados:
    def __init__(self):
        self.pasta = os.path.join('teste_bd', 'arquivos')  # Caminho corrigido
        self.urls_zip = [
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2023/1T2023.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2023/2T2023.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2023/3T2023.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2023/4T2023.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/1T2024.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/2T2024.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/3T2024.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
            "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
        ]

    def baixar_arquivos(self):
        try:
            os.makedirs(self.pasta, exist_ok=True)

            for url in self.urls_zip:
                nome_arquivo = os.path.join(self.pasta, url.split("/")[-1])
                print(f"Baixando {nome_arquivo}...")

                resposta = requests.get(url, stream=True)
                resposta.raise_for_status()

                with open(nome_arquivo, "wb") as arquivo:
                    for chunk in resposta.iter_content(chunk_size=8192):
                        if chunk:
                            arquivo.write(chunk)

                print(f"Concluído: {nome_arquivo}")

            print("Todos os downloads foram concluídos!")
            return True
            
        except Exception as e:
            print(f"Erro ao baixar arquivos: {e}")
            return False

    def extrair_arquivos_zip(self):
        try:
            for arquivo in os.listdir(self.pasta):
                if arquivo.endswith('.zip'):
                    caminho_zip = os.path.join(self.pasta, arquivo)

                    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                        zip_ref.extractall(self.pasta)

                    os.remove(caminho_zip)
                    print(f'Arquivo {arquivo} extraído e removido')

            # Corrigindo o caminho para renomear o arquivo
            caminho_antigo = os.path.join(self.pasta, '2t2023.csv')
            caminho_novo = os.path.join(self.pasta, '2T2023.csv')
            if os.path.exists(caminho_antigo):
                os.rename(caminho_antigo, caminho_novo)

            print("Todos os arquivos ZIP foram processados!")
            return True
            
        except Exception as e:
            print(f"Erro ao extrair arquivos: {e}")
            return False

    def tratar_demonstracoes_contabeis(self):
        try:
            for arquivo in os.listdir(self.pasta):
                if re.match(r'^[1-4]T\d{4}\.CSV$', arquivo, re.IGNORECASE):
                    caminho_arquivo = os.path.join(self.pasta, arquivo)
                    df = pd.read_csv(caminho_arquivo, delimiter=';')
                    
                    if 'DATA' in df.columns:
                        df['DATA'] = pd.to_datetime(df['DATA'])
                    
                    for coluna in ['VL_SALDO_INICIAL', 'VL_SALDO_FINAL']:
                        if coluna in df.columns:
                            df[coluna] = df[coluna].str.replace(',', '.', regex=False).astype(float)
                    
                    for coluna in ['REG_ANS', 'CD_CONTA_CONTABIL']:
                        if coluna in df.columns:
                            df[coluna] = df[coluna].astype(str)
                    
                    df.to_csv(caminho_arquivo, sep=';', index=False)
                    print(f"{arquivo}: Datas e valores monetários convertidos. Arquivo salvo.")
            return True
            
        except Exception as e:
            print(f"Erro ao processar demonstrações: {e}")
            return False

    def tratar_relatorio_cadop(self):
        try:
            caminho_relatorio = os.path.join(self.pasta, 'Relatorio_cadop.csv')
            if os.path.exists(caminho_relatorio):
                df_cadop = pd.read_csv(caminho_relatorio, delimiter=';')

                colunas_str = ['Registro_ANS', 'CNPJ', 'CEP', 'DDD', 'Telefone', 'Fax', 'Regiao_de_Comercializacao']
                df_cadop[colunas_str] = df_cadop[colunas_str].astype(str)

                df_cadop['DDD'] = df_cadop['DDD'].str.replace('.', '').str[:2]
                df_cadop['Telefone'] = df_cadop['Telefone'].str.replace('.', '').str[:-1]
                df_cadop['Data_Registro_ANS'] = pd.to_datetime(df_cadop['Data_Registro_ANS'])

                df_cadop.to_csv(caminho_relatorio, sep=';', index=False)
                print("Relatorio_cadop.csv processado com sucesso!")
                return True
            else:
                print("Arquivo Relatorio_cadop.csv não encontrado")
                return False
                
        except Exception as e:
            print(f"Erro ao processar relatório: {e}")
            return False
    
    def run(self):
        print("Iniciando processamento de dados...")
        if self.baixar_arquivos():
            if self.extrair_arquivos_zip():
                if self.tratar_demonstracoes_contabeis():
                    self.tratar_relatorio_cadop()
        print("Processamento concluído!")
```

# Docker-compose

Agora para conseguirmos executar nosso próximo script python precisamos de executar nosso docker-compose, responsável por subir o nosso banco de dados dentro de um contâiner

#### [docker-compose.yml](intuitivecare/teste_bd/docker-compose.yml)

Relembrando também que nosso docker-compose ele sobe com as variáveis de um arquivo .env então não se esqueça de configurar os valores do seu .env

```bash
# Comando utilizado para subir o docker-compose, lembrando que para subir o docker-compose, deve estar na mesma pasta do arquivo.
docker-compose up -d
```


```
version: '3.8'
services:
  postgres:
    image: postgres:latest
    env_file:
      - .env
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
volumes:
  postgres_data:
```

# Scripts_sql

#### [scripts_sql.py](intuitivecare/teste_bd/scripts_sql.py)

Aqui após subirmos nosso docker-compose temos nosso script python que realiza a criação das tabelas e importação dos arquivos csv para o banco de dados

```python
# postgres_importer.py
import psycopg2
import os
from dotenv import load_dotenv

class PostgresDataImporter:
    def __init__(self):
        load_dotenv()
        self.pasta = os.path.join('teste_bd', 'arquivos')
        self.CSV_FILES = [
            os.path.join(self.pasta, '1T2023.csv'),
            os.path.join(self.pasta, '2T2023.csv'),
            os.path.join(self.pasta, '3T2023.csv'),
            os.path.join(self.pasta, '4T2023.csv'),
            os.path.join(self.pasta, '1T2024.csv'),
            os.path.join(self.pasta, '2T2024.csv'),
            os.path.join(self.pasta, '3T2024.csv'),
            os.path.join(self.pasta, '4T2024.csv')
        ]
        self.RELATORIO_CSV = os.path.join(self.pasta, 'Relatorio_cadop.csv')
        self.conn = None

    def conectar_banco(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                port=5432
            )
            print("DB_HOST:", os.getenv("DB_HOST"))
            return self.conn
        except Exception as e:
            print(f"Erro ao conectar ao banco: {e}")
            return None

    def criar_tabelas(self, conn):
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS demonstracoes_contabeis (
                        id SERIAL PRIMARY KEY,
                        DATA DATE NOT NULL,
                        REG_ANS varchar(15),
                        CD_CONTA_CONTABIL varchar(15),
                        DESCRICAO text,
                        VL_SALDO_INICIAL DECIMAL(15, 2),
                        VL_SALDO_FINAL DECIMAL(15,2)
                    );
                """)

                cur.execute("""
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
                """)
                conn.commit()
                print("Tabelas criadas com sucesso!")
        except Exception as e:
            print(f"Erro ao criar tabela: {e}")
            conn.rollback()

    def importar_csv(self, conn, csv_path):
        try:
            with conn.cursor() as cur, open(csv_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(
                    """COPY demonstracoes_contabeis(
                        DATA, 
                        REG_ANS, 
                        CD_CONTA_CONTABIL, 
                        DESCRICAO, 
                        VL_SALDO_INICIAL, 
                        VL_SALDO_FINAL
                    ) FROM STDIN WITH (FORMAT CSV, DELIMITER ';', HEADER)""",
                    f
                )
            conn.commit()
            print(f"{os.path.basename(csv_path)} importado com sucesso!")
        except Exception as e:
            print(f"Erro em {os.path.basename(csv_path)}: {e}")
            conn.rollback()

    def importar_relatorio_csv(self, conn):
        try:
            with conn.cursor() as cur, open(self.RELATORIO_CSV, 'r', encoding='utf-8') as f:
                cur.copy_expert(
                    """COPY operadoras_de_plano_de_saude_ativas(
                        Registro_ANS, 
                        CNPJ, 
                        Razao_Social, 
                        Nome_Fantasia, 
                        Modalidade, 
                        Logradouro, 
                        Numero, 
                        Complemento, 
                        Bairro,
                        Cidade, 
                        UF, 
                        CEP, 
                        DDD, 
                        Telefone, 
                        Fax, 
                        Endereco_eletronico, 
                        Representante, 
                        Cargo_Representante, 
                        Regiao_de_Comercializacao, 
                        Data_Registro_ANS
                    ) FROM STDIN WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '\\',  HEADER)""",
                    f
                )
            conn.commit()
            print(f"{os.path.basename(self.RELATORIO_CSV)} importado com sucesso!")
        except Exception as e:
            print(f"Erro ao importar {os.path.basename(self.RELATORIO_CSV)}: {e}")
            conn.rollback()

    def run(self):
        conn = self.conectar_banco()
        if conn:
            self.criar_tabelas(conn)
            for csv_file in self.CSV_FILES:
                self.importar_csv(conn, csv_file)
            self.importar_relatorio_csv(conn)
            conn.close()
```

# Queries

#### [query.sql](intuitivecare/teste_bd/)

Realização de ambas queries analíticas

```
-- Aqui temos ambas as querys análiticas realizadas utilizando CTEs

with operadoras_com_mais_despesas as (
	select 
		dc.reg_ans,
		SUM(vl_saldo_inicial - vl_saldo_final) AS total_despesa,
		dc.descricao,
		dc.data

	from 
		demonstracoes_contabeis as dc
	where 
		dc.descricao = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR '
		AND data >= '2024-10-01'  -- Início do 4º trimestre
        AND data <= '2024-12-31'   -- Fim do 4º trimestre
	group by 
        reg_ans, descricao, data
    order by
    	total_despesa desc
)
select
	reg_ans,
    total_despesa,
    descricao,
    data
FROM 
    operadoras_com_mais_despesas
limit 10

-- Segunda query

with operadoras_com_mais_despesas_ult_ano as (
	select 
		dc.reg_ans,
		SUM(vl_saldo_inicial - vl_saldo_final) AS total_despesa,
		dc.descricao,
		EXTRACT(YEAR FROM MAX(data)) AS ano

	from 
		demonstracoes_contabeis as dc
	where 
		dc.descricao = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS  DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR '
	group by 
        reg_ans, descricao
    order by
    	total_despesa desc
)
select
	reg_ans,
    total_despesa,
    descricao,
    ano
FROM 
    operadoras_com_mais_despesas_ult_ano
limit 10
```

# Main.py
#### [main.py](intuivecare/main.py)

Este é nosso main.py que executa todos os 3 primeiros steps do teste

```python
from webscraping import WebScraper
from etl import PDFProcessor
from teste_bd.download import ProcessandoDados
from teste_bd.scripts_sql import PostgresDataImporter

WebScraper().run()
PDFProcessor().run()
ProcessandoDados().run()
PostgresDataImporter().run()
```

# Interface Web

#### [busca_textual.py](intuitivecare/busca_textual.py)

Este script executa nosso servidor flask, após executar este script, nosso frontend estará pronto para comunicar com o banco de dados e conseguir realizar a busca textual na lista de cadastros

```bash
# Link para acessar nosso frontend
http://127.0.0.1:5500/frontend/templates/index.html
```

```python
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

import psycopg2 
import psycopg2.extras
# from psycopg2 import sql
import os

from teste_bd.scripts_sql import PostgresDataImporter   
from dotenv import load_dotenv

load_dotenv()

template_dir = os.path.abspath('../frontend/templates')

app = Flask(__name__, template_folder=template_dir)
CORS(app)

conexao = PostgresDataImporter()
conexao.conectar_banco

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/buscar', methods=['GET'])
def buscar_operadoras():
    termo_busca = request.args.get('q', '') 
    
    if not termo_busca:
        return jsonify({"error": "Parâmetro 'q' é obrigatório"}), 400
    
    conn = conexao.conectar_banco()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        query = """
            SELECT * FROM operadoras_de_plano_de_saude_ativas
            WHERE razao_social ILIKE %s
               OR nome_fantasia ILIKE %s
               OR cnpj ILIKE %s
               OR registro_ans ILIKE %s
            ORDER BY razao_social
            LIMIT 50
        """
        termo_pattern = f"%{termo_busca}%"
        cur.execute(query, (termo_pattern, termo_pattern, termo_pattern, termo_pattern))
        
        resultados = cur.fetchall()
        return jsonify([dict(item) for item in resultados])
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)
```
