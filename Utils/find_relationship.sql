DEFINE SCHEMA_NAME = 'CLARO';

WITH params AS (
  SELECT UPPER('&SCHEMA_NAME') AS schema_name FROM dual
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
    'RESOURCE_ACTION','CUSTOMER',
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

-- verifica quais dessas tabelas existem no schema informado
existing_tables AS (
  SELECT ut.table_name
  FROM all_tables ut
  JOIN params p ON 1=1
  WHERE ut.owner = p.schema_name
    AND ut.table_name IN (SELECT UPPER(table_name) FROM selected_tables)
),

-- relations: faz o casamento por constraint_name e por posição entre ALL_CONS_COLUMNS
relations_raw AS (
  SELECT DISTINCT
       UPPER(pk.owner)   AS pk_owner,
       UPPER(pk.table_name)   AS parent_table,
       UPPER(fk.owner)   AS fk_owner,
       UPPER(fk.table_name)   AS child_table,
       fk.constraint_name AS fk_constraint,
       pk.constraint_name AS pk_constraint
  FROM params p
  JOIN all_constraints fk
    ON fk.constraint_type = 'R'
   AND fk.owner = p.schema_name               -- FK constraint owner = schema informado (child side)
  JOIN all_cons_columns fkcol
    ON fk.owner = fkcol.owner
   AND fk.constraint_name = fkcol.constraint_name
  JOIN all_constraints pk
    ON fk.r_owner = pk.owner
   AND fk.r_constraint_name = pk.constraint_name
  JOIN all_cons_columns pkcol
    ON pk.owner = pkcol.owner
   AND pk.constraint_name = pkcol.constraint_name
   AND pkcol.position = fkcol.position       -- casamento por posição das colunas da constraint
  WHERE UPPER(fk.table_name) IN (SELECT table_name FROM existing_tables)
    AND UPPER(pk.table_name) IN (SELECT table_name FROM existing_tables)
),

-- reduz para parent->child únicos
relations AS (
  SELECT DISTINCT parent_table, child_table
  FROM relations_raw
),

-- raízes: tabelas que não aparecem como child (entre as existentes selecionadas)
roots AS (
  SELECT et.table_name
  FROM existing_tables et
  WHERE et.table_name NOT IN (SELECT child_table FROM relations)
),

-- CTE recursiva (declare colunas)
tree(table_name, lvl, path) AS (
  -- anchor
  SELECT table_name, 0 AS lvl, table_name AS path
  FROM roots

  UNION ALL

  -- recursão: expande filhos
  SELECT r.child_table, t.lvl + 1,
         t.path || '->' || r.child_table
  FROM tree t
  JOIN relations r
    ON r.parent_table = t.table_name
  WHERE t.lvl < 500
),

-- depth: maior nível alcançado por tabela (max path depth)
depths AS (
  SELECT table_name, MAX(lvl) AS depth
  FROM tree
  GROUP BY table_name
),

-- parents imediatos (pode haver múltiplos)
parents AS (
  SELECT r.child_table AS table_name,
         LISTAGG(r.parent_table, ',') WITHIN GROUP (ORDER BY r.parent_table) AS parents_list
  FROM relations r
  GROUP BY r.child_table
)

-- resultado final: lista todas as tabelas solicitadas que EXISTEM no schema com depth e pais
SELECT st.table_name,
       NVL(d.depth, 0) AS depth,
       COALESCE(p.parents_list, '-') AS parents
FROM selected_tables st
LEFT JOIN existing_tables et ON UPPER(st.table_name) = et.table_name
LEFT JOIN depths d ON UPPER(st.table_name) = d.table_name
LEFT JOIN parents p ON UPPER(st.table_name) = p.table_name
ORDER BY NVL(d.depth,0), st.table_name;
