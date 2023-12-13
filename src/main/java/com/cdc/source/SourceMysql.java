package com.cdc.source;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.cdc.sink.SinkKafka;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SourceMysql implements Source {
    private static final Logger LOG = LoggerFactory.getLogger(SourceMysql.class);

    @Override
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            List<String> columns = MetaData.tablesMetaData.get(table).get("columns");
            List<String> primaryKeys = MetaData.tablesMetaData.get(table).get("primaryKeys");
            createMysqlTable(table, columns, primaryKeys, baseParameters, tableEnv);
        });
    }

    public static void createMysqlTable(String table, List<String> columns, List<String> primaryKeys, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String mode = "";
        if (baseParameters.getMode().equalsIgnoreCase("full")) mode = "initial";
        else if (baseParameters.getMode().equalsIgnoreCase("increment")) mode = "specific-offset";
        else {
            LOG.error("Sink mode should be either full or increment");
            throw new RuntimeException("Sink mode should be either full or increment");
        }
        String mysqlCreateTableSql = "CREATE TABLE source_" + table + " (\n";
        mysqlCreateTableSql += columns.stream()
                .map(str -> "  " + str + ",\n")
                .collect(Collectors.joining());
        String offset = mode.equals("specific-offset") ? "  'scan.startup.specific-offset.file' = '" + baseParameters.getBinlogFile() + "',\n" +
                "  'scan.startup.specific-offset.pos' = '" + baseParameters.getBinlogPos() + "',\n"
                : "";
        String primaryKeyString = String.join(", ", primaryKeys);
        mysqlCreateTableSql += "  PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n) WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '" + baseParameters.getSourceHostName() + "',\n" +
                "  'port' = '" + baseParameters.getSourcePort() + "',\n" +
                "  'username' = '" + baseParameters.getSourceUserName() + "',\n" +
                "  'password' = '" + baseParameters.getSourcePassword() + "',\n" +
                "  'database-name' = '" + baseParameters.getSourceDatabaseName() + "',\n" +
                "  'table-name' = '" + table + "',\n" +
                "  'debezium.snapshot.mode' = 'when_needed',\n" +
                "  'scan.startup.mode' = '" + mode + "',\n" +
                offset +
                "  'scan.incremental.snapshot.chunk.size' = '" + baseParameters.getFlinkChunkSize() + "',\n" +
                "  'debezium.timezone' = 'Asia/Shanghai'\n" +
                ")";
        tableEnv.executeSql(mysqlCreateTableSql);
    }
}
