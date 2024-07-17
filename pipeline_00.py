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


#Função que faz o download do googledrive:

def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

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







if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    #baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)

    for caminho_do_arquivo in lista_de_arquivos:
        data_frame_duckdb = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformar(data_frame_duckdb)
        salvar_no_postgres(pandas_df_transformado, "vendas_calculado")
