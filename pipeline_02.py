# Melhora do código: Evita salvar dados repetidos a cada execução.

#Imports

import os
import gdown # faz o dowload do googledrive
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame
from datetime import datetime

load_dotenv()

# Função que conecta ao banco onde os dados serão persistidos:
def conectar_banco():
    """ Conecta ao banco de dados DuckDB; Cria o banco se o mesmo não existir"""
    return duckdb.connect(database='duckdb.db', read_only=False)

# Cria a tabela com o nome do arquivo inserido e o timestamp da inserção:
def inicializar_tabela(con):
    """Cria a tabela se a mesma não existir."""
    con.execute("""
                CREATE TABLE IF NOT EXISTS historico_arquivos(
                nome_arquivo VARCHAR,
                horario_processamento TIMESTAMP)""")


# Sempre que um arquivo for salvo no Postgres ele também será salvo no DuckDB:
def registra_arquivo(con, nome_arquivo):
    """Registra um novo arquivo no banco de dados com o horário atual."""
    con.execute("""
                INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
                VALUES(?,?)""",
                (nome_arquivo, datetime.now()))
    
# Faz select nos registros inseridos:
def arquivos_processados(con):
    """Retorna um set com nomes de todos os arquivos já processados."""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())



#Função que faz o download do googledrive:

def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)


# Lista os arquivos e seus respectivos tipos (csv, json e parquet):
def listar_arquivos_e_tipos(diretorio):
    """Lista arquivos e identifica se são CSV, JSON ou Parquet."""
    arquivos_e_tipos = []
    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(".csv") or arquivo.endswith(".json") or arquivo.endswith(".parquet"):
            caminho_completo = os.path.join(diretorio, arquivo)
            tipo = arquivo.split(".")[-1]
            arquivos_e_tipos.append((caminho_completo, tipo))
    return arquivos_e_tipos


# Lê cada tipo de arquivo:
def ler_arquivo(caminho_do_arquivo, tipo):
    """Lê o arquivo de acordo com seu tipo e retorna um DataFrame."""
    if tipo == 'csv':
        return duckdb.read_csv(caminho_do_arquivo)
    elif tipo == 'json':
        return pd.read_json(caminho_do_arquivo)
    elif tipo == 'parquet':
        return pd.read_parquet(caminho_do_arquivo)
    else:
        raise ValueError(f"Tipo de arquivo não suportado: {tipo}")
    

# Verificar (listar) somente os arquivos CSV que foram baixados:
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
            
        
    return arquivos_csv

# Função para ler um arquivo CSV e retornar um DataFrame duckdb:
def ler_csv(caminho_do_arquivo):
    return duckdb.read_csv(caminho_do_arquivo)


# Função para adicionar uma coluna de total de vendas
def transformar(df: DuckDBPyRelation) -> DataFrame:
    # Executa a consulta SQL que inclui a nova coluna, operando sobre a tabela virtual
    df_transformado = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    # Remove o registro da tabela virtual para limpeza
    return df_transformado

# Função para converter o Duckdb em Pandas e salvar o DataFrame no PostgreSQL:

def salvar_no_postgres(df_duckdb, tabela):
  DATABASE_URL = os.getenv("DATABASE_URL")  
  engine = create_engine(DATABASE_URL)
  df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


logs = []
def pipeline():
    url_pasta = 'https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    #baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivos_processados(con)

    for caminho_do_arquivo in lista_de_arquivos:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in processados:
            data_frame_duckdb = ler_csv(caminho_do_arquivo)
            df_transformado = transformar(data_frame_duckdb)
            salvar_no_postgres(df_transformado, "vendas_calculado")
            registra_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo.")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado anteriormente.")
            logs.append(f"Arquivo {nome_arquivo} ja foi processado anteriormente.")
    return logs



if __name__ == "__main__":
    pipeline()
