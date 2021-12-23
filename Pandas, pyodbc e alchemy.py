# Importar bibliotecas
import pandas as pd 
import numpy as np 
import time # Para cronometrar o envio
from sqlalchemy import create_engine, event # Responsável pela engine (o que vai possibilitar conectar no SQL)
from urllib.parse import quote_plus # Facilita a criação da URL pra engine

# Conexão com o banco de dados
conn =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=***SERVIDOR***;DATABASE=***DATABASE***;UID=***USUARIO***;PWD=***SENHA***" # Dados para conectar
quoted = quote_plus(conn) # Facilitador da URL
new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted) # URL de conexão
engine = create_engine(new_con) # Criação da engine

# Configuração e acompanhamento do cursor(pelo terminal)
@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("Enviando...")
    if executemany:
        cursor.fast_executemany = True

# Definir nome da tabela que será criada no banco de dados
table_name = '***NOME DA TABELA***'
# Leitura da tabela, utilizando Pandas
db = pd.read_excel(r'***LOCAL***')

# Coletar tempo de inicio
s = time.time()
# Enviar o dataframe para o SQL
db.to_sql(table_name, engine, if_exists = 'replace', chunksize = None) # Alterar o chunksize conforme necessidade (limitação do servidor)
# Printar ao término, quanto tempo levou
print(time.time() - s)
