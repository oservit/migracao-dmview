WITH params AS (
    SELECT 
        'OI' AS schema_origem, 
        'CLARO' AS schema_destino 
    FROM dual
),
static_tables AS (
    SELECT UPPER(column_value) AS table_name FROM TABLE(sys.odcivarchar2list(
        'CIRCUIT_PRIORITY', 'CIRCUIT_TYPE', 'COMMENT_LABEL', 
        'DEVICE_DATA_FILE_TYPE', 'PORT_LINK_TYPE', 'TB___ALARMS__TB', 
        'TB___MODELS__TB', 'TB___OBJECTS__TB', 'VENDORS'
    ))
),
selected_tables AS (
    SELECT UPPER(column_value) AS table_name FROM TABLE(sys.odcivarchar2list(
        'CIRCUIT', 'CIRCUIT_COMMENT', 'CIRCUIT_DEVICE_END', 'CIRCUIT_DEVICE_END_PORT',
        'CIRCUIT_END', 'CIRCUIT_EVENT', 'CIRCUIT_EVENT_HISTORY', 'CIRCUIT_FILTER',
        'CIRCUIT_FILTER_PROPERTY', 'CIRCUIT_OBJECT', 'CIRCUIT_PATH_SECTION',
        'CIRCUIT_PORT_END', 'CIRCUIT_PORT_END_TS', 'CIRCUIT_PROTECTION',
        'CIRCUIT_SITUATION_BY_DEVICE', 'CROSS_PATH_SECTION', 'CROSS_PATH_SECTION_TS',
        'CUSTOM_MAPPING_PATH_SECTION', 'LINK_PATH_SECTION', 'MATRIX_MAPPING_PATH_SECTION',
        'PATH', 'PATH_SECTION', 'PORT_LINK', 'PORT_PATH_SECTION',
        'SDH_MAPPING_DM800_PATH_SECTION', 'SDH_MAPPING_PATH_SECTION', 'VCG_MAPPING_PATH_SECTION',
        'RESOURCE_ACTION', 'CUSTOMER', 'CUSTOMER_PRIORITY', 'SERVICE',
        'HTTP_CREDENTIALS', 'NETCONF_CREDENTIALS', 'PCGA_CREDENTIALS',
        'PROVISIONING_RESOURCE', 'SFTP_CREDENTIALS', 'SNMP', 'SNMPV1_2',
        'SSH_CREDENTIALS', 'TB___MUXS__TB', 'TB___PROXIES__TB', 'TELNET_CREDENTIALS',
        'DEVICE_DATA_FILE', 'MATRIX', 'MATRIX_MAPPING', 'METRO_PORT_VLANS',
        'METRO_VLAN_GROUP', 'METRO_VLAN_GROUP_VLANS', 'PORTS_TESTS', 'SDH_MAPPING',
        'SDH_MAPPING_DM800', 'SLOT', 'STP_CFG', 'TB___CFG_MUXS__TB', 'TB___CFG_PORTS__TB',
        'TB___MUXS_ALARMS__TB', 'TB___OBJS_CUSTOM_LABEL__TB', 'TB___PORTS__TB',
        'TB___PORTS_ALARMS__TB', 'TB___ST_MUXS__TB', 'TB___ST_PORTS__TB', 'TIME_SLOT',
        'TIME_SLOT_MAPPING', 'TIME_SLOT_PORT', 'VCG_MAPPING', 'VLAN_CFG',
        'MANAGEMENT_VLAN', 'MS_SPRING_SQUELCH', 'STP_PROTECTED_VLAN_GROUPS',
        'CONNECTION', 'CONNECTION_LINKS', 'GRAPHIC', 'GROUP_NODE', 'DEVICE_NODE',
        'LINK', 'NODE', 'SHORTCUT_NODE'
    ))
),
existing_tables AS (
    SELECT ut.table_name FROM all_tables ut CROSS JOIN params p 
    WHERE ut.owner = p.schema_origem AND ut.table_name IN (SELECT table_name FROM selected_tables)
),
pk_info AS (
    SELECT acc.table_name,
           CASE 
             WHEN COUNT(*) = 1 THEN 'TRIM(CAST(src.' || MIN(acc.column_name) || ' AS VARCHAR2(64)))'
             ELSE 'LOWER(RAWTOHEX(STANDARD_HASH(' || LISTAGG('TRIM(CAST(src.' || acc.column_name || ' AS VARCHAR2(100)))', ' || ''|'' || ') 
                  WITHIN GROUP (ORDER BY acc.position) || ', ''MD5'')))'
           END AS pk_expr
    FROM all_constraints ac
    JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
    CROSS JOIN params p WHERE ac.owner = p.schema_destino AND ac.constraint_type = 'P'
    GROUP BY acc.table_name
),
fk_constraints_mapped AS (
    SELECT fk.table_name AS child_table, pk.table_name AS parent_table, fk.constraint_name,
           'j' || ROW_NUMBER() OVER (ORDER BY fk.table_name, fk.constraint_name) AS join_alias,
           CASE 
             WHEN st.table_name IS NOT NULL THEN
                LISTAGG('j_alias.' || pkcol.column_name || ' = src.' || fkcol.column_name, ' AND ') WITHIN GROUP (ORDER BY fkcol.position)
             WHEN COUNT(*) = 1 THEN
                'j_alias.ERP_CODE = TRIM(CAST(src.' || MIN(fkcol.column_name) || ' AS VARCHAR2(64)))'
             ELSE
                'j_alias.ERP_CODE = LOWER(RAWTOHEX(STANDARD_HASH(' || 
                LISTAGG('TRIM(CAST(src.' || fkcol.column_name || ' AS VARCHAR2(100)))', ' || ''|'' || ') 
                WITHIN GROUP (ORDER BY fkcol.position) || ', ''MD5'')))'
           END AS join_condition
    FROM all_constraints fk
    JOIN all_cons_columns fkcol ON fk.owner = fkcol.owner AND fk.constraint_name = fkcol.constraint_name
    JOIN all_constraints pk ON fk.r_owner = pk.owner AND fk.r_constraint_name = pk.constraint_name
    JOIN all_cons_columns pkcol ON pk.owner = pkcol.owner AND fk.r_constraint_name = pkcol.constraint_name AND fkcol.position = pkcol.position
    LEFT JOIN static_tables st ON pk.table_name = st.table_name
    CROSS JOIN params p WHERE fk.constraint_type = 'R' AND fk.owner = p.schema_destino
    GROUP BY fk.table_name, pk.table_name, fk.constraint_name, st.table_name
),
column_fk_mapping AS (
    SELECT fk.table_name, fkcol.column_name, fcm.join_alias, pkcol.column_name AS parent_pk_col
    FROM all_constraints fk
    JOIN all_cons_columns fkcol ON fk.owner = fkcol.owner AND fk.constraint_name = fkcol.constraint_name
    JOIN fk_constraints_mapped fcm ON fk.constraint_name = fcm.constraint_name
    JOIN all_cons_columns pkcol ON fk.r_owner = pkcol.owner AND fk.r_constraint_name = pkcol.constraint_name AND fkcol.position = pkcol.position
),
final_joins AS (
    SELECT child_table,
           LISTAGG('LEFT JOIN ' || (SELECT schema_destino FROM params) || '.' || parent_table || ' ' || join_alias || ' ON ' || REPLACE(join_condition, 'j_alias', join_alias), CHR(10))
           WITHIN GROUP (ORDER BY join_alias) AS join_clauses
    FROM fk_constraints_mapped GROUP BY child_table
),
migration_components AS (
    SELECT et.table_name, pki.pk_expr,
           LISTAGG(atc.column_name, ',' || CHR(10)) WITHIN GROUP (ORDER BY atc.column_id) AS all_columns_list,
           LISTAGG(CASE 
                WHEN atc.column_name = 'ERP_CODE' THEN pki.pk_expr || ' AS ERP_CODE'
                WHEN cfm.column_name IS NOT NULL THEN cfm.join_alias || '.' || cfm.parent_pk_col || ' AS ' || atc.column_name
                ELSE 'src.' || atc.column_name 
           END, ',' || CHR(10)) WITHIN GROUP (ORDER BY atc.column_id) AS select_columns_mapped
    FROM existing_tables et
    JOIN all_tab_cols atc ON atc.table_name = et.table_name
    JOIN pk_info pki ON pki.table_name = et.table_name
    LEFT JOIN column_fk_mapping cfm ON cfm.table_name = et.table_name AND cfm.column_name = atc.column_name
    CROSS JOIN params p WHERE atc.owner = p.schema_destino
    GROUP BY et.table_name, pki.pk_expr
),
tree(child_table, parent_table, lvl) AS (
    SELECT child_table, parent_table, 1 FROM (SELECT DISTINCT child_table, parent_table FROM fk_constraints_mapped)
    UNION ALL
    SELECT r.child_table, t.parent_table, t.lvl + 1
    FROM (SELECT DISTINCT child_table, parent_table FROM fk_constraints_mapped) r
    JOIN tree t ON r.parent_table = t.child_table WHERE t.lvl < 20
) CYCLE child_table SET is_cycle TO '1' DEFAULT '0',
depths AS (
    SELECT table_name, MAX(depth) as depth FROM (
        SELECT child_table as table_name, lvl as depth FROM tree WHERE is_cycle = '0'
        UNION ALL SELECT table_name, 0 FROM existing_tables
    ) GROUP BY table_name
)
SELECT 
    mc.table_name, d.depth, '-',
    'INSERT INTO ' || (SELECT schema_destino FROM params) || '.' || mc.table_name || ' (' || CHR(10) || mc.all_columns_list || CHR(10) || ')' || CHR(10) ||
    'SELECT ' || CHR(10) || mc.select_columns_mapped || CHR(10) ||
    'FROM ' || (SELECT schema_origem FROM params) || '.' || mc.table_name || ' src' || CHR(10) ||
    fj.join_clauses || CHR(10) ||
    'WHERE NOT EXISTS (SELECT 1 FROM ' || (SELECT schema_destino FROM params) || '.' || mc.table_name || ' dest WHERE dest.ERP_CODE = ' || mc.pk_expr || ');' as script_sql
FROM migration_components mc
JOIN depths d ON mc.table_name = d.table_name
LEFT JOIN final_joins fj ON fj.child_table = mc.table_name
ORDER BY d.depth, mc.table_name;