from socketserver import ThreadingUDPServer
import pandas as pd
import pyodbc
import json
import re
from dotenv import load_dotenv
import os

# ================================
# 1) Ler e limpar os dados do arquivo JSON
# ================================
file_path = "Siglario_csvjson.json"  # Certifique-se de que este arquivo esteja na mesma pasta

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Nova etapa de sanitização profunda:
    # Itera sobre os dados e remove caracteres de controle e não imprimíveis.
    sanitized_data = []
    for item in data:
        new_item = {}
        for key, value in item.items():
            if isinstance(value, str):
                # Remove caracteres de controle e nulos
                # Este é o tratamento mais robusto
                new_item[key] = re.sub(r'[\x00-\x1F\x7F]', '', value)
            else:
                new_item[key] = value
        sanitized_data.append(new_item)

    # Cria o DataFrame do pandas a partir dos dados limpos
    df = pd.DataFrame(sanitized_data)

except FileNotFoundError:
    raise FileNotFoundError(f"Erro: O arquivo '{file_path}' não foi encontrado.")
except json.JSONDecodeError:
    raise json.JSONDecodeError(f"Erro: O arquivo '{file_path}' não é um JSON válido.")

if df.empty:
    raise ValueError("O DataFrame está vazio. Verifique o conteúdo do arquivo JSON.")

# Padroniza nomes de colunas
df.columns = df.columns.str.strip().str.upper()

# Mantém apenas as colunas que interessam
df = df[["ABREVIATURA", "SIGNIFICADO"]]

# Remove linhas totalmente vazias
df.dropna(how="all", inplace=True)

# Remove linhas sem abreviatura OU sem significado
df.dropna(subset=["ABREVIATURA", "SIGNIFICADO"], inplace=True)

# Remove espaços em branco
df["ABREVIATURA"] = df["ABREVIATURA"].astype(str).str.strip()
df["SIGNIFICADO"] = df["SIGNIFICADO"].astype(str).str.strip()

# Exclui linhas que ficaram vazias depois do "strip"
df = df[(df["ABREVIATURA"] != "") & (df["SIGNIFICADO"] != "")]

print("Pré-visualização dos dados filtrados do JSON:")
print(df.head(), "\nTotal de linhas após limpeza:", len(df))

# ================================
# 2) Conectar ao SQL Server
# ================================

load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

conn_str = f"""
DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
UID={username};
PWD={password};
TrustServerCertificate=yes;
"""

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Conectado ao SQL Server com sucesso!")

    # ================================
    # 3) Inserção em lotes (chunks)
    # ================================
    print("Iniciando a inserção dos registros em lotes...")

    # Converte o DataFrame para uma lista de tuplas
    valores = list(df.itertuples(index=False, name=None))

    # Define o tamanho do lote
    batch_size = 1000

    # Cria a instrução SQL com o número certo de placeholders
    sql_base = "INSERT INTO Cad_Siglario (ABREVIATURA, SIGNIFICADO) VALUES "
    placeholders_template = ", ".join(["(?, ?)"] * batch_size)

    # Processa os dados em lotes
    for i in range(0, len(valores), batch_size):
        chunk = valores[i:i + batch_size]

        # Achata a lista de tuplas para ter uma única lista de parâmetros
        params = [item for sublist in chunk for item in sublist]

        # Ajusta a instrução SQL para o último lote, se ele for menor
        if len(chunk) < batch_size:
            placeholders_final = ", ".join(["(?, ?)"] * len(chunk))
            sql_final = sql_base + placeholders_final
            cursor.execute(sql_final, params)
        else:
            sql_batch = sql_base + placeholders_template
            cursor.execute(sql_batch, params)

    conn.commit()
    print(f"Todos os {len(valores)} registros foram inseridos com sucesso!")

except Exception as e:
    print("Erro durante a execução:", e)
    conn.rollback()

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("Conexão encerrada.")

 