package com.cdc.source;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SourceKafka implements Source {

    @Override
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            List<String> columns = MetaData.tablesMetaData.get(table).get("columns");
            List<String> primaryKeys = MetaData.tablesMetaData.get(table).get("primaryKeys");
            createKafkaTable(table, columns, primaryKeys, baseParameters, tableEnv);
        });
    }

    public static void createKafkaTable(String table, List<String> columns, List<String> primaryKeys, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        String primaryKeyString = String.join(", ", primaryKeys);
        String kafkaCreateTableSql = "CREATE TABLE source_" + table + " (\n";
        kafkaCreateTableSql += columns.stream()
                .map(column -> "  " + column)
                .collect(Collectors.joining(",\n"))
                + ",\n   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n";
        kafkaCreateTableSql += ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + baseParameters.getKafkaTopic() + table + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.group.id' = 'flinkcdc-group',\n" +
                "  'properties.bootstrap.servers' = '" + baseParameters.getKafkaAddress() + "',\n" +
                " 'format' = 'canal-json'\n" +
                ")";
        tableEnv.executeSql(kafkaCreateTableSql);
    }
}
