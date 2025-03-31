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