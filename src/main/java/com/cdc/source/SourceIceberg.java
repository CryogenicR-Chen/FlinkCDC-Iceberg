package com.cdc.source;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.cdc.sink.SinkIceberg;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceIceberg implements Source {

    static SinkIceberg sinkIceberg = new SinkIceberg();

    @Override
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String databaseName = baseParameters.getIcebergSourceDatabase();
        String catalogName = baseParameters.getIcebergSourceCatalog();
        createIcebergCatalog(databaseName, catalogName, baseParameters, tableEnv);
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            List<String> columns = MetaData.tablesMetaData.get(table).get("columns");
            List<String> primaryKeys = MetaData.tablesMetaData.get(table).get("primaryKeys");
            createIcebergTable(catalogName, databaseName, columns, primaryKeys, table, baseParameters, tableEnv);
        });
    }

    public static void createIcebergCatalog(String databaseName, String catalogName, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        sinkIceberg.createIcebergCatalog(databaseName, catalogName, baseParameters, tableEnv);
    }

    public static void createIcebergTable(String catalogName, String databaseName,
                                          List<String> columns, List<String> primaryKeys, String table, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        sinkIceberg.createIcebergTable(catalogName, databaseName, columns, primaryKeys, table, baseParameters, tableEnv);
    }
}
