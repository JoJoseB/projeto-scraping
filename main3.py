import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import re

# Configurações
url_principal = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/levantamento-de-precos-de-combustiveis-ultimas-semanas-pesquisadas"
pasta_base = "planilhas_anp"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

def criar_pasta(year):
    caminho = os.path.join(pasta_base, str(year))
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        print(f"Pasta para {year} criada: {caminho}")
    return caminho

def baixar_arquivo(url, ano):
    pasta_destino = criar_pasta(ano)
    nome_arquivo = os.path.join(pasta_destino, os.path.basename(url))
    
    if os.path.exists(nome_arquivo):
        print(f"Arquivo {os.path.basename(nome_arquivo)} já existe. Pulando...")
        return

    try:
        resposta = requests.get(url, headers=headers)
        resposta.raise_for_status()
        
        with open(nome_arquivo, 'wb') as f:
            f.write(resposta.content)
        print(f"Arquivo {ano} baixado: {os.path.basename(nome_arquivo)}")
        
    except Exception as e:
        print(f"Erro ao baixar {url}: {str(e)}")

def encontrar_links():
    try:
        resposta = requests.get(url_principal, headers=headers)
        resposta.raise_for_status()
        
        soup = BeautifulSoup(resposta.text, 'html.parser')
        
        # Padrão para 2022 e 2023
        padrao = re.compile(r'resumo_semanal_lpc_(2022|2023).*\.xlsx$', re.IGNORECASE)
        
        links = soup.find_all('a', href=padrao)
        
        if not links:
            print("Nenhum link encontrado!")
            return []
            
        return [(urljoin(url_principal, link['href']), 
                re.search(r'2022|2023', link['href']).group()) 
                for link in links]
        
    except Exception as e:
        print(f"Erro ao processar a página: {str(e)}")
        return []

def main():
    if not os.path.exists(pasta_base):
        os.makedirs(pasta_base)
    
    links = encontrar_links()
    
    if links:
        print("\nArquivos encontrados:")
        for url, ano in links:
            print(f"{ano}: {os.path.basename(url)}")
            baixar_arquivo(url, ano)

if __name__ == "__main__":
    main()