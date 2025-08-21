import pandas as pd
from sqlalchemy import create_engine
import os

# --- Configuração da Conexão com o MySQL ---
# Formato: 'mysql+mysqlconnector://usuario:senha@host/database'
db_user = 'root'
db_password = '02112003Pa$' 
db_host = 'localhost'
db_name = 'analise_ecommerce'

# String de conexão
connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}'
engine = create_engine(connection_string)

# Caminho para os arquivos de dados ---
data_path = 'data' # Pasta onde os arquivos CSV estão armazenados

# Carrega cada CSV para uma tabela no MySQL 
for filename in os.listdir(data_path):
    if filename.endswith('.csv'):
        table_name = filename.replace('olist_', '').replace('_dataset.csv', '')
        filepath = os.path.join(data_path, filename)
        
        print(f"Carregando {filename} para a tabela {table_name}...")
        
        df = pd.read_csv(filepath)
        
        # Usando o pandas para inserir os dados no SQL de forma eficiente
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
        print(f"Tabela {table_name} carregada com sucesso!")

print("\nProcesso de carga de dados finalizado.")