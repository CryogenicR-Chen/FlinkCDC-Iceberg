package com.cdc.sink;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SinkKafka implements Sink {
    private static final Logger LOG = LoggerFactory.getLogger(SinkKafka.class);

    @Override
    public void sink(String source, StatementSet statementSet, BaseParameters baseParameters, StreamTableEnvironment tableEnv) {
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            if (source.equals("mysql")) {
                Table mysqlTable = tableEnv.from("source_" + table);
                statementSet.addInsert("sink_" + table, mysqlTable);
            } else {
                LOG.error("Do Not Support {} to {}", source, "kafka");
                throw new RuntimeException("Unsupported Source or Sink");
            }
        });
    }

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
        String kafkaCreateTableSql = "CREATE TABLE sink_" + table + " (\n";
        kafkaCreateTableSql += columns.stream()
                .map(column -> "  " + column)
                .collect(Collectors.joining(",\n"))
                + ",\n   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n";
        kafkaCreateTableSql += ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + baseParameters.getKafkaTopic() + table + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.bootstrap.servers' = '" + baseParameters.getKafkaAddress() + "',\n" +
                " 'format' = 'canal-json'\n" +
                ")";
        tableEnv.executeSql(kafkaCreateTableSql);
    }
}
