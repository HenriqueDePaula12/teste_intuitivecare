# postgres_importer.py
import psycopg2
import os
from dotenv import load_dotenv

class PostgresDataImporter:
    def __init__(self):
        load_dotenv()
        self.pasta = os.path.join('teste_bd', 'arquivos')  # Caminho corrigido
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