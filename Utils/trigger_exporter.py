import oracledb
import os

# --- CONFIGURAÇÕES DE ACESSO ---
DB_USER = "OI"
DB_PASS = "O1#998dm#v1Ew12#"
DB_HOST = "172.26.132.24"
DB_PORT = 1521
DB_SERV = "dmview12"

# Nomes dos arquivos
SQL_FILE = "trigger_generator.sql"
OUTPUT_DIR = "output_triggers"

def main():
    if not os.path.exists(SQL_FILE):
        print(f"Erro: Arquivo '{SQL_FILE}' não encontrado.")
        return

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_query = f.read().strip()
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]

    connection = None
    try:
        print(f"Conectando para gerar triggers...")
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            service_name=DB_SERV
        )
        
        cursor = connection.cursor()
        cursor.execute(sql_query)

        count = 0
        for row in cursor:
            count += 1
            table_name = row[0]
            trigger_sql = row[1]

            if not trigger_sql: continue

            # Nomeia o arquivo como TRG_BI_TABELA.sql
            filename = f"TRG_BI_{table_name}.sql"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"-- Trigger para populamento de Sequence | Tabela: {table_name}\n")
                f.write(trigger_sql)
                # Adiciona a barra "/" essencial para rodar blocos PL/SQL no Oracle
                f.write("\n/\n")

            print(f" [OK] Gerado: {filename}")

        print(f"\nFinalizado! {count} triggers geradas na pasta '{OUTPUT_DIR}'.")

    except oracledb.Error as e:
        print(f"\nErro Oracle: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()