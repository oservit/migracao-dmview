DEFINE SCHEMA_NAME = 'CLARO';

WITH params AS (
  SELECT UPPER('&SCHEMA_NAME') AS schema_name
    FROM dual
),

selected_tables AS (
  SELECT column_value AS table_name FROM TABLE(sys.odcivarchar2list(
    'CIRCUIT',
    'CIRCUIT_COMMENT',
    'CIRCUIT_DEVICE_END',
    'CIRCUIT_DEVICE_END_PORT',
    'CIRCUIT_END',
    'CIRCUIT_EVENT',
    'CIRCUIT_EVENT_HISTORY',
    'CIRCUIT_FILTER',
    'CIRCUIT_FILTER_PROPERTY',
    'CIRCUIT_OBJECT',
    'CIRCUIT_PATH_SECTION',
    'CIRCUIT_PORT_END',
    'CIRCUIT_PORT_END_TS',
    'CIRCUIT_PROTECTION',
    'CIRCUIT_SITUATION_BY_DEVICE',
    'CROSS_PATH_SECTION',
    'CROSS_PATH_SECTION_TS',
    'CUSTOM_MAPPING_PATH_SECTION',
    'LINK_PATH_SECTION',
    'MATRIX_MAPPING_PATH_SECTION',
    'PATH',
    'PATH_SECTION',
    'PORT_LINK',
    'PORT_PATH_SECTION',
    'SDH_MAPPING_DM800_PATH_SECTION',
    'SDH_MAPPING_PATH_SECTION',
    'VCG_MAPPING_PATH_SECTION',
    'RESOURCE_ACTION',
    'CUSTOMER',
    'CUSTOMER_PRIORITY',
    'SERVICE',
    'HTTP_CREDENTIALS',
    'NETCONF_CREDENTIALS',
    'PCGA_CREDENTIALS',
    'PROVISIONING_RESOURCE',
    'SFTP_CREDENTIALS',
    'SNMP',
    'SNMPV1_2',
    'SSH_CREDENTIALS',
    'TB___MUXS__TB',
    'TB___PROXIES__TB',
    'TELNET_CREDENTIALS',
    'DEVICE_DATA_FILE',
    'MATRIX',
    'MATRIX_MAPPING',
    'METRO_PORT_VLANS',
    'METRO_VLAN_GROUP',
    'METRO_VLAN_GROUP_VLANS',
    'PORTS_TESTS',
    'SDH_MAPPING',
    'SDH_MAPPING_DM800',
    'SLOT',
    'STP_CFG',
    'TB___CFG_MUXS__TB',
    'TB___CFG_PORTS__TB',
    'TB___MUXS_ALARMS__TB',
    'TB___OBJS_CUSTOM_LABEL__TB',
    'TB___PORTS__TB',
    'TB___PORTS_ALARMS__TB',
    'TB___ST_MUXS__TB',
    'TB___ST_PORTS__TB',
    'TIME_SLOT',
    'TIME_SLOT_MAPPING',
    'TIME_SLOT_PORT',
    'VCG_MAPPING',
    'VLAN_CFG',
    'MANAGEMENT_VLAN',
    'MS_SPRING_SQUELCH',
    'STP_PROTECTED_VLAN_GROUPS',
    'CONNECTION',
    'CONNECTION_LINKS',
    'GRAPHIC',
    'GROUP_NODE',
    'DEVICE_NODE',
    'LINK',
    'NODE',
    'SHORTCUT_NODE'
  ))
),

existing_tables AS (
  SELECT ut.table_name
  FROM all_tables ut
  JOIN params p ON 1=1
  WHERE ut.owner = p.schema_name
    AND ut.table_name IN (SELECT UPPER(table_name) FROM selected_tables)
),

-- 1. Identifica o nome da PK (assumindo PK de coluna única)
primary_keys AS (
    SELECT
        acc.table_name,
        acc.column_name AS pk_column
    FROM all_constraints ac
    JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
    WHERE ac.owner = (SELECT schema_name FROM params)
      AND ac.constraint_type = 'P' -- Primary Key
      AND acc.table_name IN (SELECT table_name FROM existing_tables)
),

-- Relações Detalhadas
relations_detailed AS (
  SELECT
    UPPER(fk.table_name) AS child_table,
    UPPER(pk.table_name) AS parent_table,
    fkcol.column_name AS fk_column,
    ROW_NUMBER() OVER (PARTITION BY fk.table_name ORDER BY fkcol.position) AS rn
  FROM params p
  JOIN all_constraints fk ON fk.constraint_type = 'R' AND fk.owner = p.schema_name
  JOIN all_cons_columns fkcol ON fk.owner = fkcol.owner AND fk.constraint_name = fkcol.constraint_name
  JOIN all_constraints pk ON fk.r_owner = pk.owner AND fk.r_constraint_name = pk.constraint_name
  JOIN all_cons_columns pkcol ON pk.owner = pkcol.owner AND pk.constraint_name = pkcol.constraint_name
    AND pkcol.position = fkcol.position
  WHERE UPPER(fk.table_name) IN (SELECT table_name FROM existing_tables)
    AND UPPER(pk.table_name) IN (SELECT table_name FROM existing_tables)
),

-- 2. Montagem dos JOINS e Colunas de SELECT, excluindo a PK
migration_components AS (
    SELECT 
        et.table_name,
        pk.pk_column,
        -- Lista de colunas do INSERT INTO (Exclui PK)
        LISTAGG(CASE WHEN atc.column_name = pk.pk_column THEN NULL ELSE atc.column_name END, ',' || CHR(10)) 
            WITHIN GROUP (ORDER BY atc.column_id) AS all_columns_list,
        
        -- Monta o SELECT de colunas (Aplica a lógica de mapeamento: FK, ERP_CODE, ou valor direto. Exclui PK)
        LISTAGG(
            CASE 
                WHEN atc.column_name = pk.pk_column THEN NULL -- Exclui a PK do SELECT
                WHEN rd.fk_column IS NOT NULL THEN 
                    'j' || rd.rn || '.ID AS ' || atc.column_name 
                WHEN atc.column_name = 'ERP_CODE' THEN
                    'src.' || pk.pk_column || ' AS ' || atc.column_name -- ERP_CODE = PK antiga
                ELSE 
                    'src.' || atc.column_name 
            END, ',' || CHR(10)) 
            WITHIN GROUP (ORDER BY atc.column_id) AS select_columns_mapped,

        -- Monta a lista de JOINS para as FKs
        LISTAGG(
            CASE 
                WHEN rd.fk_column IS NOT NULL THEN 
                    'LEFT JOIN ' || rd.parent_table || ' j' || rd.rn || ' ON j' || rd.rn || '.ERP_CODE = src.' || rd.fk_column
                ELSE NULL 
            END, CHR(10)) WITHIN GROUP (ORDER BY atc.column_id) AS join_clauses
    FROM existing_tables et
    JOIN all_tab_cols atc ON atc.owner = (SELECT schema_name FROM params) AND atc.table_name = et.table_name
    JOIN primary_keys pk ON pk.table_name = et.table_name
    LEFT JOIN (SELECT DISTINCT child_table, fk_column, parent_table, rn FROM relations_detailed) rd 
        ON rd.child_table = et.table_name AND rd.fk_column = atc.column_name
    GROUP BY et.table_name, pk.pk_column
),

-- 3. Geração do script final (VARCHAR2)
migration_scripts AS (
  SELECT
    mc.table_name,
    CAST(
      'INSERT INTO ' || mc.table_name || CHR(10) ||
      '(' || CHR(10) || 
      REPLACE(mc.all_columns_list, ',' || CHR(10) || ',', ',' || CHR(10)) || -- Remove vírgulas duplas de colunas excluídas
      CHR(10) || ')' || CHR(10) ||
      'SELECT' || CHR(10) ||
      REPLACE(mc.select_columns_mapped, ',' || CHR(10) || ',', ',' || CHR(10)) || -- Remove vírgulas duplas de colunas excluídas
      CHR(10) ||
      'FROM ' || (SELECT schema_name FROM params) || '.' || mc.table_name || ' src' || CHR(10) ||
      COALESCE(mc.join_clauses, '') || CHR(10) ||
      'WHERE NOT EXISTS (' || CHR(10) ||
      '    SELECT 1 FROM ' || mc.table_name || ' dest WHERE dest.' || mc.pk_column || ' = src.' || mc.pk_column || CHR(10) ||
      ');' || CHR(10) ||
      'COMMIT;'
    AS VARCHAR2(4000)) AS script_varchar
  FROM migration_components mc
),

-- Relações e Depth (simplificadas)
relations AS (
  SELECT DISTINCT child_table, parent_table FROM relations_detailed
),

roots AS (
  SELECT et.table_name FROM existing_tables et
  WHERE et.table_name NOT IN (SELECT child_table FROM relations)
),

tree(table_name, lvl, path) AS (
  SELECT table_name, 0 AS lvl, table_name AS path FROM roots
  UNION ALL
  SELECT r.child_table, t.lvl + 1, t.path || '->' || r.child_table
  FROM tree t
  JOIN relations r ON r.parent_table = t.table_name
  WHERE t.lvl < 500
),

depths AS (
  SELECT table_name, MAX(lvl) AS depth FROM tree GROUP BY table_name
),

parents AS (
  SELECT r.child_table AS table_name,
          LISTAGG(r.parent_table, ', ') WITHIN GROUP (ORDER BY r.parent_table) AS parents_list
  FROM relations r GROUP BY r.child_table
)

-- Resultado Final
SELECT
    st.table_name AS "TABELA",
    NVL(d.depth, 0) AS "DEPTH",
    COALESCE(p.parents_list, '-') AS "PAIS_RELACIONADOS",
    ms.script_varchar AS "SCRIPT_MIGRACAO_SQL" 
FROM selected_tables st
LEFT JOIN existing_tables et ON UPPER(st.table_name) = et.table_name
LEFT JOIN depths d ON UPPER(st.table_name) = d.table_name
LEFT JOIN parents p ON UPPER(st.table_name) = p.table_name
LEFT JOIN migration_scripts ms ON UPPER(st.table_name) = ms.table_name
ORDER BY "DEPTH", "TABELA";