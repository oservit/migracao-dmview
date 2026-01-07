
WITH params AS (
    SELECT 'CLARO' AS schema_destino FROM dual
),
-- 1. Tabelas que possuem a coluna ERP_CODE
tables_with_erp AS (
    SELECT table_name 
    FROM all_tab_cols 
    WHERE owner = (SELECT schema_destino FROM params) 
      AND column_name = 'ERP_CODE'
),
-- 2. Busca recursiva de hierarquia com tratamento de ciclo
hierarchy (table_name, parent_table, lvl) AS (
    -- Root: Tabelas que não são filhas de ninguém
    SELECT table_name, CAST(NULL AS VARCHAR2(128)), 1
    FROM all_tables
    WHERE owner = (SELECT schema_destino FROM params)
      AND table_name NOT IN (
          SELECT table_name FROM all_constraints 
          WHERE constraint_type = 'R' AND owner = (SELECT schema_destino FROM params)
      )
    UNION ALL
    -- Recursive: Tabelas filhas
    SELECT ac.table_name, h.table_name, h.lvl + 1
    FROM all_constraints ac
    JOIN hierarchy h ON ac.r_constraint_name = (
        SELECT constraint_name FROM all_constraints 
        WHERE table_name = h.table_name 
          AND constraint_type = 'P' 
          AND owner = (SELECT schema_destino FROM params)
    )
    WHERE ac.constraint_type = 'R' 
      AND ac.owner = (SELECT schema_destino FROM params)
)
-- Tratamento de Ciclo (Evita o erro ORA-32044)
CYCLE table_name SET is_cycle TO '1' DEFAULT '0'
-- 3. Agrupa por tabela pegando o nível mais alto (mais profundo)
SELECT 
    'DELETE FROM ' || (SELECT schema_destino FROM params) || '.' || t.table_name || 
    ' WHERE ERP_CODE IS NOT NULL;' as delete_command
FROM tables_with_erp t
LEFT JOIN (
    SELECT table_name, MAX(lvl) as max_lvl 
    FROM hierarchy 
    GROUP BY table_name
) h ON t.table_name = h.table_name
ORDER BY NVL(h.max_lvl, 0) DESC, t.table_name ASC;
