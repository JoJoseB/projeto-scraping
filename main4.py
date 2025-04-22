import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
import warnings
from openpyxl import load_workbook

# Configurações do PostgreSQL
POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "dataforce",
    "user": "dataforce",
    "password": "dataforce",
    "port": "5432"
}

PASTA_BASE = "planilhas_anp"
IGNORAR_LINHAS = 9

def criar_conexao():
    try:
        conn_str = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        return create_engine(conn_str)
    except Exception as e:
        print(f"Erro na conexão: {e}")
        return None

def padronizar_colunas(df):
    # Padronizar nomes das colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
    )
    return df

def atualizar_esquema(engine, df):
    inspector = inspect(engine)
    colunas_existentes = inspector.get_columns('precos_combustiveis')
    colunas_tabela = [col['name'] for col in colunas_existentes]
    
    with engine.connect() as conn:
        for coluna in df.columns:
            if coluna not in colunas_tabela and coluna != 'id':
                try:
                    conn.execute(text(f"ALTER TABLE precos_combustiveis ADD COLUMN {coluna} TEXT"))
                    conn.commit()
                    print(f"Coluna {coluna} adicionada à tabela")
                except Exception as e:
                    print(f"Erro ao adicionar coluna {coluna}: {e}")
                    continue

def processar_planilhas():
    engine = criar_conexao()
    if not engine:
        return
    
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    
    # Criar tabela base se não existir
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS precos_combustiveis (
                id SERIAL PRIMARY KEY
            )
        """))
        conn.commit()

    for ano in [2022, 2023]:
        pasta_ano = os.path.join(PASTA_BASE, str(ano))
        
        if not os.path.exists(pasta_ano):
            continue
            
        for arquivo in Path(pasta_ano).glob("*.xlsx"):
            print(f"\nProcessando: {arquivo.name}")
            
            try:
                wb = load_workbook(filename=arquivo, read_only=True, data_only=True)
                wb.close()
                
                xls = pd.ExcelFile(arquivo)
                
                for sheet_name in xls.sheet_names:
                    try:
                        df = pd.read_excel(
                            xls,
                            sheet_name=sheet_name,
                            skiprows=IGNORAR_LINHAS,
                            dtype=str,
                            engine='openpyxl'
                        )
                        
                        # Pré-processamento
                        df = df.dropna(how='all')
                        df = padronizar_colunas(df)
                        
                        # Converter colunas numéricas
                        for col in df.columns:
                            if 'valor' in col or 'preco' in col:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            elif 'data' in col:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                        
                        # Atualizar schema da tabela
                        atualizar_esquema(engine, df)
                        
                        # Inserir dados
                        df.to_sql(
                            name='precos_combustiveis',
                            con=engine,
                            if_exists='append',
                            index=False
                        )
                        
                    except Exception as e:
                        print(f"Erro na aba {sheet_name}: {str(e)[:100]}...")
                        continue
                    
            except Exception as e:
                print(f"Erro no arquivo {arquivo.name}: {str(e)[:100]}...")
                continue

    engine.dispose()
    print("\nProcessamento concluído!")

if __name__ == "__main__":
    processar_planilhas()