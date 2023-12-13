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

public class SinkMixedIceberg implements Sink {
    static SinkIceberg sinkIceberg = new SinkIceberg();
    private static final Logger LOG = LoggerFactory.getLogger(SinkMixedIceberg.class);

    @Override
    public void sink(String source, StatementSet statementSet, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String catalogName = baseParameters.getMixedIcebergCatalog();
        String databaseName = baseParameters.getMixedIcebergDatabase();
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            String sql = "";
            if (source.equals("kafka")) {
                Table kafkaTable = tableEnv.from("source_" + table);
                if (baseParameters.getIcebergLimited()) LimitRateUtil.limitRate(kafkaTable, table, tableEnv);
                String tableName = baseParameters.getIcebergLimited() ? "temp_view_" + table : "source_" + table;
                sql = "INSERT INTO " + catalogName + "." + databaseName + "." + table + "_iceberg SELECT * FROM " + tableName;

            } else if (source.equals("iceberg")) {
                String sourceCatalogName = baseParameters.getIcebergSourceCatalog();
                String sourceDatabaseName = baseParameters.getIcebergSourceDatabase();
                sql = "INSERT INTO " + catalogName + "." + databaseName + "." + table + "_iceberg SELECT * FROM " + sourceCatalogName + "." + sourceDatabaseName + "." + table + "_iceberg";
            } else {
                LOG.error("Do Not Support {} to {}", source, "mixed-iceberg");
                throw new RuntimeException("Unsupported Source or Sink");
            }
            statementSet.addInsertSql(sql);
        });
    }

    @Override
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String databaseName = baseParameters.getMixedIcebergDatabase();
        String catalogName = baseParameters.getMixedIcebergCatalog();
        createIcebergMixedCatalog(databaseName, catalogName, baseParameters, tableEnv);
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            List<String> columns = MetaData.tablesMetaData.get(table).get("columns");
            List<String> primaryKeys = MetaData.tablesMetaData.get(table).get("primaryKeys");
            createMixedIcebergTable(catalogName, databaseName, columns, primaryKeys, table, baseParameters, tableEnv);
        });
    }

    public static void createIcebergMixedCatalog(String databaseName, String catalogName, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {


        tableEnv.executeSql("CREATE CATALOG " + catalogName + " WITH (\n" +
                "  'type'='arctic',\n" +
                "  'property-version'='1',\n" +
                "  'metastore.url' = 'thrift://sloth-commerce-test2.jd.163.org:18150/" + catalogName + "'\n" +
                ")");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + catalogName + "." + databaseName);
    }

    public static void createMixedIcebergTable(String catalogName, String databaseName,
                                               List<String> columns, List<String> primaryKeys, String table, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        sinkIceberg.createIcebergTable(catalogName, databaseName, columns, primaryKeys, table, baseParameters, tableEnv);
    }
}
