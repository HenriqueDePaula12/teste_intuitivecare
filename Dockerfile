# Usa a imagem oficial do Python 3.12 com slim (versão reduzida)
FROM python:3.12-slim

# Primeiro copia apenas o requirements.txt para aproveitar o cache de camadas
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o resto dos arquivos
COPY . .

# Comando para rodar a aplicação
# (Altere para o comando correto para executar seu main.py)
CMD ["python", "main.py"]