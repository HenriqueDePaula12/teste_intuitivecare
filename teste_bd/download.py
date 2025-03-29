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