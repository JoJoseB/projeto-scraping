from sqlalchemy import create_engine, text, inspect

# Parâmetros de conexão ao banco de dados
conn_params = {
    'dbname': 'dataforce',
    'user': 'dataforce',
    'password': 'dataforce',
    'host': 'localhost',
    'port': '5432'
}

# Criar a engine do SQLAlchemy
engine = create_engine(
    f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
)

# Lista das tabelas originais
tabelas = ['uf', 'precos_combustiveis', 'Arrecadaçao por Cnae', 'arrecadacao por setor ']

# Obter as colunas de cada tabela
colunas = {}
with engine.connect() as conn:
    for tabela in tabelas:
        result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :tabela
        """), {"tabela": tabela.strip().lower()})
        
        colunas[tabela] = result.fetchall()

# Criar a definição da nova tabela com aspas duplas nos nomes das colunas
definicao_colunas = []
for tabela, cols in colunas.items():
    for col in cols:
        nome_coluna = f'"{tabela}_{col[0]}"'
        definicao_colunas.append(f"{nome_coluna} {col[1]}")

definicao_tabela = ', '.join(definicao_colunas)

# Imprimir a query para verificação
print(f"CREATE TABLE dados_integrados ({definicao_tabela})")

# Criar a tabela 'dados_integrados' e inserir os dados
with engine.begin() as conn:
    conn.execute(text(f"CREATE TABLE dados_integrados ({definicao_tabela})"))
    
    # Inserir os dados usando CROSS JOIN
    query_insercao = f"""
        INSERT INTO dados_integrados
        SELECT *
        FROM "{tabelas[0]}"
        CROSS JOIN "{tabelas[1]}"
        CROSS JOIN "{tabelas[2]}"
        CROSS JOIN "{tabelas[3]}"
    """
    conn.execute(text(query_insercao))

print("Tabela 'dados_integrados' criada com sucesso!")
