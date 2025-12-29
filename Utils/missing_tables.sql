DEFINE SCHEMA_NAME = 'CLARO';

WITH params AS (
  SELECT UPPER('&SCHEMA_NAME') AS schema_name FROM dual
),

-- Sua lista original de tabelas (o escopo atual)
selected_tables AS (
  SELECT UPPER(column_value) AS table_name FROM TABLE(sys.odcivarchar2list(
    'CIRCUIT', 'CIRCUIT_COMMENT', 'CIRCUIT_DEVICE_END', 'CIRCUIT_DEVICE_END_PORT',
    'CIRCUIT_END', 'CIRCUIT_EVENT', 'CIRCUIT_EVENT_HISTORY', 'CIRCUIT_FILTER',
    'CIRCUIT_FILTER_PROPERTY', 'CIRCUIT_OBJECT', 'CIRCUIT_PATH_SECTION',
    'CIRCUIT_PORT_END', 'CIRCUIT_PORT_END_TS', 'CIRCUIT_PROTECTION',
    'CIRCUIT_SITUATION_BY_DEVICE', 'CROSS_PATH_SECTION', 'CROSS_PATH_SECTION_TS',
    'CUSTOM_MAPPING_PATH_SECTION', 'LINK_PATH_SECTION', 'MATRIX_MAPPING_PATH_SECTION',
    'PATH', 'PATH_SECTION', 'PORT_LINK', 'PORT_PATH_SECTION',
    'SDH_MAPPING_DM800_PATH_SECTION', 'SDH_MAPPING_PATH_SECTION',
    'VCG_MAPPING_PATH_SECTION', 'RESOURCE_ACTION', 'CUSTOMER',
    'CUSTOMER_PRIORITY', 'SERVICE', 'HTTP_CREDENTIALS', 'NETCONF_CREDENTIALS',
    'PCGA_CREDENTIALS', 'PROVISIONING_RESOURCE', 'SFTP_CREDENTIALS',
    'SNMP', 'SNMPV1_2', 'SSH_CREDENTIALS', 'TB___MUXS__TB',
    'TB___PROXIES__TB', 'TELNET_CREDENTIALS', 'DEVICE_DATA_FILE',
    'MATRIX', 'MATRIX_MAPPING', 'METRO_PORT_VLANS', 'METRO_VLAN_GROUP',
    'METRO_VLAN_GROUP_VLANS', 'PORTS_TESTS', 'SDH_MAPPING',
    'SDH_MAPPING_DM800', 'SLOT', 'STP_CFG', 'TB___CFG_MUXS__TB',
    'TB___CFG_PORTS__TB', 'TB___MUXS_ALARMS__TB', 'TB___OBJS_CUSTOM_LABEL__TB',
    'TB___PORTS__TB', 'TB___PORTS_ALARMS__TB', 'TB___ST_MUXS__TB',
    'TB___ST_PORTS__TB', 'TIME_SLOT', 'TIME_SLOT_MAPPING', 'TIME_SLOT_PORT',
    'VCG_MAPPING', 'VLAN_CFG', 'MANAGEMENT_VLAN', 'MS_SPRING_SQUELCH',
    'STP_PROTECTED_VLAN_GROUPS', 'CONNECTION', 'CONNECTION_LINKS',
    'GRAPHIC', 'GROUP_NODE', 'DEVICE_NODE', 'LINK', 'NODE', 'SHORTCUT_NODE'
  ))
),

-- Cruza as FKs das suas tabelas para achar pais fora da lista
missing_parents AS (
  SELECT DISTINCT
    UPPER(pk.table_name) AS parent_table_missing,
    UPPER(fk.table_name) AS referenced_by_table,
    fkcol.column_name AS via_fk_column
  FROM all_constraints fk
  JOIN all_cons_columns fkcol ON fk.owner = fkcol.owner AND fk.constraint_name = fkcol.constraint_name
  JOIN all_constraints pk ON fk.r_owner = pk.owner AND fk.r_constraint_name = pk.constraint_name
  JOIN params p ON fk.owner = p.schema_name
  WHERE fk.constraint_type = 'R' -- Foreign Key
    AND fk.table_name IN (SELECT table_name FROM selected_tables) -- Tabela filha está na lista
    AND pk.table_name NOT IN (SELECT table_name FROM selected_tables) -- Tabela pai NÃO está na lista
)

-- Resultado Final: Lista de tabelas que você PRECISA adicionar para manter a integridade
SELECT 
    parent_table_missing AS "TABELA_PAI_FALTANTE",
    LISTAGG(referenced_by_table || ' (' || via_fk_column || ')', ', ' || CHR(10)) 
        WITHIN GROUP (ORDER BY referenced_by_table) AS "REQUISITADA_PELAS_TABELAS"
FROM missing_parents
GROUP BY parent_table_missing
ORDER BY parent_table_missing;