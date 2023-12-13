package com.cdc.sink;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.cdc.util.LimitRateUtil;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class SinkIceberg implements Sink {
    private static final Logger LOG = LoggerFactory.getLogger(SinkIceberg.class);

    @Override
    public void sink(String source, StatementSet statementSet, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String databaseName = baseParameters.getIcebergSinkDatabase();
        String catalogName = baseParameters.getIcebergSinkCatalog();
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            String sql = "";
            if (source.equals("kafka") || source.equals("mysql")) {
                Table kafkaTable = tableEnv.from("source_" + table);
                if (baseParameters.getIcebergLimited()) LimitRateUtil.limitRate(kafkaTable, table, tableEnv);
                String tableName = baseParameters.getIcebergLimited() ? "temp_view_" + table : "source_" + table;
                sql = "INSERT INTO " + catalogName + "." + databaseName + "." + table + "_iceberg SELECT * FROM " + tableName;

            } else if (source.equals("iceberg")) {
                String sourceCatalogName = baseParameters.getIcebergSourceCatalog();
                String sourceDatabaseName = baseParameters.getIcebergSourceDatabase();
                sql = "INSERT INTO " + catalogName + "." + databaseName + "." + table + "_iceberg SELECT * FROM " + sourceCatalogName + "." + sourceDatabaseName + "." + table + "_iceberg";
            } else {
                LOG.error("Do Not Support {} to {}", source, "iceberg");
                throw new RuntimeException("Unsupported Source or Sink");
            }
            statementSet.addInsertSql(sql);
        });
    }

    @Override
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String databaseName = baseParameters.getIcebergSinkDatabase();
        String catalogName = baseParameters.getIcebergSinkCatalog();
        createIcebergCatalog(databaseName, catalogName, baseParameters, tableEnv);
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            List<String> columns = MetaData.tablesMetaData.get(table).get("columns");
            List<String> primaryKeys = MetaData.tablesMetaData.get(table).get("primaryKeys");
            createIcebergTable(catalogName, databaseName, columns, primaryKeys, table, baseParameters, tableEnv);
        });
    }

    public void createIcebergCatalog(String databaseName, String catalogName, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {

        tableEnv.executeSql("CREATE CATALOG " + catalogName + " WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'uri' = '" + baseParameters.getIcebergURI() + "',\n" +
                "  'warehouse' = 'hdfs://hz11-trino-arctic-0.jd.163.org:8020" + baseParameters.getIcebergWarehouse() + "'\n" +
                ")");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + catalogName + "." + databaseName);
    }

    public void createIcebergTable(String catalogName, String databaseName,
                                   List<String> columns, List<String> primaryKeys, String table, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String primaryKeyString = String.join(", ", primaryKeys);
        String icebergCreateTableSql = "CREATE TABLE IF NOT EXISTS " + catalogName + "." + databaseName + "." + table + "_iceberg (\n";
        for (int i = 0; i < columns.size(); i++) {
            icebergCreateTableSql += "  " + columns.get(i);
            if (i < columns.size() - 1) {
                icebergCreateTableSql += ",\n";
            } else {
                String optimizer = baseParameters.getOptimizeGroupName().equals("default") ? "" : " 'self-optimizing.enabled' = 'true',\n 'self-optimizing.group' = '" + baseParameters.getOptimizeGroupName() + "',\n";
                String rewriteFileSize = baseParameters.getIcebergRewriteFileSize().equals(0L) ? "" : " 'write.target-file-size-bytes' = '" + baseParameters.getIcebergRewriteFileSize() + "',\n";
                icebergCreateTableSql += ",\n" +
                        "   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n) " +
                        "WITH (\n" +
                        optimizer + rewriteFileSize +
                        "  'format-version' = '2',\n" +
                        "  'write.metadata.metrics.default' = 'full',\n" +
                        "  'table-expire.enabled' = 'false',\n" +
                        "  'write.upsert.enabled' = '" + baseParameters.getIcebergWriteUpsertEnable() + "'\n" +
                        ")";
            }

        }
        tableEnv.executeSql(icebergCreateTableSql);
    }

}
