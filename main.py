import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np


options = Options()
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Remote(
    command_executor="http://localhost:4444/wd/hub",
    options=chrome_options
)

DB_CONFIG = {
    'user': 'dataforce',
    'password': 'dataforce',
    'host': 'localhost',
    'port': '5432',
    'database': 'dataforce'
}
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


driver.get('https://dados.gov.br/dados/conjuntos-dados/boletim-de-arrecadacao-dos-tributos-estaduais')
driver.find_element(By.XPATH, "/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/header").click()
time.sleep(2)
elemento = driver.find_element(By.XPATH, "/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]/div[1]/span[2]").text
time.sleep(2)
data = elemento.split(": ")[1].strip()
dia, mes, ano = data.split('/')
data_formatada = f"{ano}{mes}{dia}"
url = f"https://www.confaz.fazenda.gov.br/{data_formatada}-sig-att.xls"
arquivo =  url.split('/')[-1]
response = requests.get(url, verify=False)
print(url)
driver.quit()
if response.status_code == 200:
    with open(arquivo, 'wb') as f:
        f.write(response.content)
    print(f"File saved to {arquivo}")
else:
    print(f"Download failed - Status code: {response.status_code}")

xls = pd.ExcelFile(arquivo)
abas_processar = xls.sheet_names
dataframes = {}

for aba in abas_processar:
    skiprows = 1 if aba != 'uf' else 0
    df = pd.read_excel(xls, sheet_name=aba, skiprows=skiprows)
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
        print(col)
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass
    df = df.drop_duplicates()
    df.to_sql(
        name=aba,
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )
