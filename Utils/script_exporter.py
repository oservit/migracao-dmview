import oracledb
import os

# --- CONFIGURAÇÕES DE ACESSO ---
DB_USER = "OI"
DB_PASS = "O1#998dm#v1Ew12#"
# Desmembre o seu DSN aqui:
DB_HOST = "172.26.132.24"
DB_PORT = 1521
DB_SERV = "dmview12"

# Nomes dos arquivos
SQL_FILE = "script_generator.sql"
OUTPUT_DIR = "output_scripts"

def main():
    # 1. Verifica se o arquivo SQL existe
    if not os.path.exists(SQL_FILE):
        print(f"Erro: Arquivo '{SQL_FILE}' não encontrado.")
        return

    # 2. Cria diretório de saída
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 3. Lê a query
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_query = f.read().strip()
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]

    connection = None
    try:
        print(f"Conectando a {DB_HOST}:{DB_PORT}/{DB_SERV}...")
        
        # FORÇANDO O MODO THIN EXPLICITAMENTE
        # Passando host, port e service_name separadamente, o driver ignora TNS_ADMIN
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            service_name=DB_SERV
        )
        
        cursor = connection.cursor()
        print("Executando gerador...")
        cursor.execute(sql_query)

        count = 0
        for row in cursor:
            count += 1
            table_name = row[0]
            depth_val  = row[1]
            sql_script = row[3]

            if not sql_script: continue

            filename = f"{count:03d}_{table_name}.sql"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"-- Ordem: {count:03d} | Tabela: {table_name} | Depth: {depth_val}\n")
                f.write(sql_script)

            print(f" [{count:03d}] Gerado: {filename}")

        print(f"\nFinalizado! {count} arquivos na pasta '{OUTPUT_DIR}'.")

    except oracledb.Error as e:
        # Se ainda assim der erro, vamos ver o que o driver está tentando fazer
        print(f"\nErro Oracle: {e}")
        print("Dica: Verifique se o Host e o Service Name estão corretos.")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()