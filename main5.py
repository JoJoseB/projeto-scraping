import psycopg2

# Parâmetros de conexão ao banco de dados
conn_params = {
    'dbname': 'dataforce',  # Substitua pelo nome do seu banco
    'user': 'dataforce',          # Substitua pelo seu usuário
    'password': 'dataforce',        # Substitua pela sua senha
    'host': 'localhost',        # Substitua pelo host, se necessário
    'port': '5432'              # Substitua pela porta, se necessário
}

# Estabelecer conexão com o banco de dados
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Lista das tabelas originais
tabelas = ['uf', 'precos_combustiveis', 'Arrecadaçao por Cnae', 'arrecadacao por setor ']

# Obter as colunas de cada tabela
colunas = {}
for tabela in tabelas:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """, (tabela,))
    colunas[tabela] = cur.fetchall()

# Criar a definição da nova tabela com aspas duplas nos nomes das colunas
definicao_colunas = []
for tabela, cols in colunas.items():
    for col in cols:
        nome_coluna = f'"{tabela}_{col[0]}"'  # Aspas duplas no nome da coluna
        definicao_colunas.append(f"{nome_coluna} {col[1]}")

definicao_tabela = ', '.join(definicao_colunas)

# Imprimir a query para verificação (opcional)
print(f"CREATE TABLE dados_integrados ({definicao_tabela})")

# Criar a tabela 'dados_integrados'
cur.execute(f"CREATE TABLE dados_integrados ({definicao_tabela})")

# Inserir os dados usando uma junção cruzada
query_insercao = f"""
    INSERT INTO dados_integrados
    SELECT *
    FROM "{tabelas[0]}"
    CROSS JOIN "{tabelas[1]}"
    CROSS JOIN "{tabelas[2]}"
    CROSS JOIN "{tabelas[3]}"
"""
cur.execute(query_insercao)

# Confirmar as alterações e fechar a conexão
conn.commit()
cur.close()
conn.close()

print("Tabela 'dados_integrados' criada com sucesso!")