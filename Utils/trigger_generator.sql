
WITH params AS (
    SELECT 'CLARO' AS schema_destino FROM dual
),
target_tables AS (
    SELECT 'CIRCUIT_END' as table_name, 'ID_CIRCUIT_END' as pk_column, 'id_generator' as seq_name FROM dual UNION ALL
    SELECT 'CIRCUIT_FILTER', 'ID_CIRCUIT_FILTER', 'id_generator' FROM dual UNION ALL
    SELECT 'CIRCUIT_PORT_END', 'ID_CIRCUIT_PORT_END', 'id_generator' FROM dual UNION ALL
    SELECT 'CUSTOMER', 'ID_CUSTOMER', 'CUSTOMER_ID_GEN' FROM dual UNION ALL
    SELECT 'DEVICE_DATA_FILE', 'ID_DEVICE_DATA_FILE', 'device_data_file_seq' FROM dual UNION ALL
    SELECT 'GRAPHIC', 'ID_GRAPHIC', 'id_generator' FROM dual UNION ALL
    SELECT 'LINK', 'ID_LINK', 'id_generator' FROM dual UNION ALL
    SELECT 'MATRIX', 'ID_MATRIX', 'MATRIX_ID_GEN' FROM dual UNION ALL
    SELECT 'NODE', 'ID_NODE', 'id_generator' FROM dual UNION ALL
    SELECT 'PATH', 'ID_PATH', 'id_generator' FROM dual UNION ALL
    SELECT 'PATH_SECTION', 'ID_PATH_SECTION', 'id_generator' FROM dual UNION ALL
    SELECT 'PORT_PATH_SECTION', 'ID_PORT_PATH_SECTION', 'id_generator' FROM dual UNION ALL
    SELECT 'SDH_MAPPING', 'ID_SDH_MAPPING', 'SDH_MAPPING_GEN' FROM dual UNION ALL
    SELECT 'SDH_MAPPING_DM800', 'ID_SDH_MAPPING_DM800', 'sdh_mapping_dm800_gen' FROM dual UNION ALL
    SELECT 'SERVICE', 'ID_SERVICE', 'SERVICE_ID_SEQ' FROM dual UNION ALL
    SELECT 'SNMP', 'ID_SNMP', 'id_generator' FROM dual UNION ALL
    SELECT 'SNMPV1_2', 'ID_SNMPV1_2', 'id_generator' FROM dual UNION ALL
    SELECT 'STP_CFG', 'ID_STP_CFG', 'id_generator' FROM dual UNION ALL
    SELECT 'TB___PROXIES__TB', 'CL___PROXY_CODE__CL', 'PROXY_CODE_GEN' FROM dual UNION ALL
    SELECT 'TIME_SLOT', 'ID_TIME_SLOT', 'id_generator' FROM dual UNION ALL
    SELECT 'VLAN_CFG', 'ID_VLAN_CFG', 'id_generator' FROM dual UNION ALL
    SELECT 'CIRCUIT_FILTER_PROPERTY', 'ID_CIRCUIT_FILTER_PROPERTY', 'id_generator' FROM dual UNION ALL
    SELECT 'CIRCUIT_PATH_SECTION', 'ID_CIRCUIT_PATH_SECTION', 'id_generator' FROM dual
)
SELECT 
    table_name,
    'CREATE OR REPLACE TRIGGER ' || (SELECT schema_destino FROM params) || '.TRG_BI_' || table_name || CHR(10) ||
    'BEFORE INSERT ON ' || (SELECT schema_destino FROM params) || '.' || table_name || CHR(10) || 
    'FOR EACH ROW' || CHR(10) ||
    'BEGIN' || CHR(10) ||
    '  IF :NEW.' || pk_column || ' IS NULL THEN' || CHR(10) ||
    '    SELECT ' || (SELECT schema_destino FROM params) || '.' || seq_name || '.NEXTVAL INTO :NEW.' || pk_column || ' FROM DUAL;' || CHR(10) ||
    '  END IF;' || CHR(10) ||
    'END;' as trigger_script
FROM target_tables;
