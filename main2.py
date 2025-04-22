import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import pandas as pd
from sqlalchemy import create_engine, inspect
import numpy as np
import warnings

# Configurações globais
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")

DB_CONFIG = {
    'user': 'dataforce',
    'password': 'dataforce',
    'host': 'localhost',
    'port': '5432',
    'database': 'dataforce'
}

def setup_driver():
    """Configura e retorna o driver do Selenium"""
    return webdriver.Remote(
        command_executor="http://localhost:4444/wd/hub",
        options=CHROME_OPTIONS
    )

def get_db_engine():
    """Cria e retorna a conexão com o PostgreSQL"""
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def download_file(driver):
    """Executa o processo de download do arquivo XLS"""
    driver.get('https://dados.gov.br/dados/conjuntos-dados/boletim-de-arrecadacao-dos-tributos-estaduais')
    driver.find_element(By.XPATH, "/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/header").click()
    time.sleep(2)
    
    elemento = driver.find_element(
        By.XPATH, "/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]/div[1]/span[2]"
    ).text
    
    data = elemento.split(": ")[1].strip()
    dia, mes, ano = data.split('/')
    data_formatada = f"{ano}{mes}{dia}"
    
    url = f"https://www.confaz.fazenda.gov.br/{data_formatada}-sig-att.xls"
    arquivo = url.split('/')[-1]
    
    # Suprimir avisos SSL
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        response = requests.get(url, verify=False)
    
    if response.status_code == 200:
        with open(arquivo, 'wb') as f:
            f.write(response.content)
        return arquivo
    raise Exception(f"Download failed - Status code: {response.status_code}")

def process_dataframe(df, engine, table_name):
    """Processa e carrega o DataFrame no banco de dados"""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[^a-z0-9_]+', '_', regex=True)
        .str.replace(r'__+', '_', regex=True)
        .str.strip('_')
    )
    
    df.replace(['', 'NA', 'N/A', 'null', 'NULL'], np.nan, inplace=True)
    
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            except:
                pass
    
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
        df = df.reindex(columns=existing_columns, fill_value=np.nan)
    
    df.drop_duplicates().to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )

def main():
    """Fluxo principal de execução"""
    driver = None
    engine = get_db_engine()
    
    try:
        driver = setup_driver()
        arquivo = download_file(driver)
        xls = pd.ExcelFile(arquivo)
        
        for aba in xls.sheet_names:
            skiprows = 1 if aba != 'uf' else 0
            df = pd.read_excel(xls, sheet_name=aba, skiprows=skiprows)
            process_dataframe(df, engine, aba)
            
    except Exception as e:
        print(f"Erro durante a execução: {str(e)}")
        
    finally:
        if driver is not None:
            try:
                driver.quit()
            except:
                pass  # Ignora erros ao fechar o driver

if __name__ == "__main__":
    main()