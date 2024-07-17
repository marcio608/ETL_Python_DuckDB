# Use a imagem base do Python 3.12
FROM python:3.9.12

# Defina o diretório de trabalho
WORKDIR /src

# Copie os arquivos da aplicação para o diretório de trabalho no contêiner
COPY . /src

# Instale as dependências listadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Exponha a porta 8501
EXPOSE 8501

# Defina o comando de entrada para iniciar a aplicação
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
# ENTRYPOINT ["python", "main.py"]
