
import oracledb
import os
import re

DB_CONFIG = {
    "user": "OI",
    "password": "O1#998dm#v1Ew12#",
    "host": "172.26.132.24",
    "port": 1521,
    "service_name": "dmview12"
}


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define as pastas de trabalho relativas à raiz do script
FOLDERS_TO_RUN = [
    os.path.join(BASE_DIR, "Triggers"),
    os.path.join(BASE_DIR, "Scripts"),
    os.path.join(BASE_DIR, "Cleanup")
]

def conectar_oracle():
    try:
        # Modo Thin explícito (não requer Oracle Client instalado)
        return oracledb.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            service_name=DB_CONFIG["service_name"]
        )
    except oracledb.Error as e:
        print(f"[ERRO] Falha ao conectar no Oracle: {e}")
        raise

def ler_arquivo_sql(caminho):
    with open(caminho, 'r', encoding='utf-8') as f:
        return f.read().strip()

def executar_pasta(caminho_completo, conn):
    nome_pasta = os.path.basename(caminho_completo)
    
    if not os.path.exists(caminho_completo):
        print(f"[AVISO] Pasta '{nome_pasta}' não encontrada em {caminho_completo}. Pulando...")
        return True

    # Lista e ordena os arquivos
    arquivos = sorted([f for f in os.listdir(caminho_completo) if f.endswith('.sql')])
    
    if not arquivos:
        print(f"[INFO] Nenhum arquivo .sql encontrado em '{nome_pasta}'.")
        return True

    print(f"\n>>> Iniciando execução da pasta: {nome_pasta} ({len(arquivos)} arquivos)")

    for arquivo in arquivos:
        caminho_sql = os.path.join(caminho_completo, arquivo)
        conteudo_sql = ler_arquivo_sql(caminho_sql)
        
        # Limpeza necessária para o driver oracledb:
        # Remove a barra "/" isolada (comum em arquivos de trigger Oracle)
        # O driver oracledb.execute() não aceita a "/" no final do bloco PL/SQL
        linhas = conteudo_sql.splitlines()
        linhas_limpas = [l for l in linhas if l.strip() != '/']
        sql_final = "\n".join(linhas_limpas).strip()
        
        # Remove ponto e vírgula final se não for um bloco PL/SQL (Trigger/Procedure)
        # O oracledb (Thin Mode) prefere comandos sem o ";" terminal
        if not (sql_final.upper().startswith("BEGIN") or sql_final.upper().startswith("CREATE OR REPLACE TRIGGER")):
            if sql_final.endswith(';'):
                sql_final = sql_final[:-1]

        cursor = conn.cursor()
        try:
            print(f"  Executando: {arquivo}...", end="", flush=True)
            cursor.execute(sql_final)
            conn.commit()
            print(" [OK]")
        except oracledb.Error as e:
            print(f" [FALHA]")
            print(f"\n" + "="*50)
            print(f"ERRO NO SCRIPT: {arquivo}")
            print(f"DETALHE: {e}")
            print("="*50)
            return False 
        finally:
            cursor.close()
            
    return True

def main():
    print("="*50)
    print("       RUNNER DE MIGRAÇÃO - DATACOM DMVIEW")
    print("="*50)
    print(f"Raiz do Projeto: {BASE_DIR}")
    print(f"Banco Destino: {DB_CONFIG['host']}/{DB_CONFIG['service_name']}")
    
    confirmacao = input("\nConfirma a execução no banco de DESTINO? (S/SIM para continuar): ").strip().upper()
    if confirmacao not in ('S', 'SIM'):
        print("Execução cancelada.")
        return

    conn = None
    try:
        conn = oracledb.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            service_name=DB_CONFIG["service_name"]
        )
        
        for caminho_pasta in FOLDERS_TO_RUN:
            if not executar_pasta(caminho_pasta, conn):
                print(f"\n[CRÍTICO] Processo interrompido.")
                break
        else:
            print("\n" + "="*50)
            print("   MIGRAÇÃO CONCLUÍDA COM SUCESSO")
            print("="*50)

    except Exception as e:
        print(f"\n[ERRO FATAL] {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
