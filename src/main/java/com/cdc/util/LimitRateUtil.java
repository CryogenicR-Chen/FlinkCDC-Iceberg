package com.cdc.util;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LimitRateUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LimitRateUtil.class);
    private static Map<String, RateLimiter> rateLimiter = new HashMap<>();

    private static Map<String, AtomicLong> allTableSize = new HashMap<>();

    public static void createRateLimiter(int limitRate) {
        double factor = (double) limitRate / 800.0;
        Arrays.stream(BaseParameters.tables).forEach(table -> {
            if (table.equals("customer")) {
                rateLimiter.put(table, RateLimiter.create(111.0 * factor));
            } else if (table.equals("district")) {
                rateLimiter.put(table, RateLimiter.create(117.0 * factor));
            } else if (table.equals("history")) {
                rateLimiter.put(table, RateLimiter.create(29.0 * factor));
            } else if (table.equals("new_order")) {
                rateLimiter.put(table, RateLimiter.create(57.0 * factor));
            } else if (table.equals("oorder")) {
                rateLimiter.put(table, RateLimiter.create(83.0 * factor));
            } else if (table.equals("order_line")) {
                rateLimiter.put(table, RateLimiter.create(830.0 * factor));
            } else if (table.equals("stock")) {
                rateLimiter.put(table, RateLimiter.create(597.0 * factor));
            } else if (table.equals("warehouse")) {
                rateLimiter.put(table, RateLimiter.create(58.0 * factor));
            } else {
                rateLimiter.put(table, RateLimiter.create(10000.0 * factor));
            }
        });
    }
    public static void testTotalSize() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Runnable task = () -> {
            long total = 0;
            for (String table : BaseParameters.tables) {
                long temp = allTableSize.get(table).get();
                total += temp;
                LOG.info("The size of {} is: " + temp + ".", table);
            }
            LOG.info("The total size is: " + total + ".");
        };
        executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);
    }

    public static void limitRate(Table table, String tableName, StreamTableEnvironment tableEnv) {
        List<String> columns = MetaData.tablesMetaData.get(tableName).get("columns");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (String column : columns) {
            String[] parts = column.split(" ");
            String name = parts[0];
            String type = parts[1];
            switch (type) {
                case "INT":
                    schemaBuilder.column(name, DataTypes.INT());
                    break;
                case "VARCHAR":
                case "CHAR":
                    schemaBuilder.column(name, DataTypes.STRING());
                    break;
                case "DECIMAL":
                    schemaBuilder.column(name, DataTypes.DECIMAL(10, 0));
                    break;
                case "FLOAT":
                    schemaBuilder.column(name, DataTypes.FLOAT());
                    break;
                case "TIMESTAMP":
                    schemaBuilder.column(name, DataTypes.TIMESTAMP());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        Schema schema = schemaBuilder.build();
        allTableSize.put(tableName, new AtomicLong(0));
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");

        DataStream<Row> dataStream = tableEnv.toChangelogStream(table, schema);
        DataStream<Row> processedStream = dataStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                allTableSize.get(tableName).addAndGet(1);
                rateLimiter.get(tableName).acquire(1);
                return value;
            }
        }, dataStream.getType());
        Table table1 = tableEnv.fromChangelogStream(processedStream, schema);
        tableEnv.createTemporaryView("temp_view_" + tableName, table1);
    }

}
