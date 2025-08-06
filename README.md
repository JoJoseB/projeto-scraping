# Projeto_dat4force

Este repositório contém o projeto para prática de scraping, desenvolvido com foco em automações com Selenium, uso de banco de dados PostgreSQL via Docker e scripts Python. Siga as instruções abaixo para configurar e executar o ambiente de desenvolvimento.

---
### Pré-requisito
O projeto foi desenvolvido em WSL (Ubuntu), devio a isso ambiente é configurado utilizando comandos do sistema operacional Linux. Juntamente a isso o projeto faz uso de python3 e containers Docker para execução plena do projeto.

---
### Clone repositório
Para execução do projeto faça clone do repositório:
```bash
git clone https://github.com/JoJoseB/projeto_dat4force.git
cd projeto_dat4force/
```

---
### Configuração do Ambiente
Configure o ambiente, o docker compose para colocar o Banco de dados postgresql no ar e o driver para ser utilizado pelo selenium garatindo abstração do host:

```bash
docker compose up -d 
```
Crie e configure o ambiente virtual python:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
após isso o essas etapas o ambiente está configurado e pode e os scripts podem ser executados.
