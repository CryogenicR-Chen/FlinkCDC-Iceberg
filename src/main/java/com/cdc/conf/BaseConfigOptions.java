/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cdc.conf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;


public class BaseConfigOptions {
    public static final ConfigOption<String> SINK_MODE =
            ConfigOptions.key("source.sink.mode").stringType().noDefaultValue();
    public static final ConfigOption<String> ICEBERG_SINK_DATABASE =
            ConfigOptions.key("iceberg.sink.database").stringType().noDefaultValue();
    public static final ConfigOption<String> ICEBERG_SINK_CATALOG =
            ConfigOptions.key("iceberg.sink.catalog").stringType().noDefaultValue();
    public static final ConfigOption<String> SINK_BINLOG_FILE =
            ConfigOptions.key("source.sink.binlog.file").stringType().noDefaultValue();
    public static final ConfigOption<String> SINK_BINLOG_POS =
            ConfigOptions.key("source.sink.binlog.pos").stringType().noDefaultValue();

    public static final ConfigOption<String> SOURCE_DATABASE_NAME =
            ConfigOptions.key("source.database.name").stringType().noDefaultValue();

    public static final ConfigOption<String> SOURCE_USERNAME =
            ConfigOptions.key("source.username").stringType().noDefaultValue();

    public static final ConfigOption<String> SOURCE_PASSWORD =
            ConfigOptions.key("source.password").stringType().noDefaultValue();

    public static final ConfigOption<String> SOURCE_HOSTNAME =
            ConfigOptions.key("source.hostname").stringType().noDefaultValue();

    public static final ConfigOption<String> SOURCE_PORT =
            ConfigOptions.key("source.port").stringType().noDefaultValue();

    public static final ConfigOption<String> ARCTIC_OPTIMIZE_GROUP_NAME =
            ConfigOptions.key("iceberg.optimize.group.name").stringType().defaultValue("default");
    public static final ConfigOption<Integer> SOURCE_PARALLELISM =
            ConfigOptions.key("source.parallelism").intType().defaultValue(4);
    public static final ConfigOption<Integer> FLINK_PORT =
            ConfigOptions.key("flink.port").intType().defaultValue(8081);
    public static final ConfigOption<Integer> FLINK_NUM_TASK_SLOTS =
            ConfigOptions.key("flink.num.task.slots").intType().defaultValue(4);
    public static final ConfigOption<Long> ICEBERG_REWRITE_FILE_SIZE_BYTE =
            ConfigOptions.key("flink.write.target-file-size-bytes").longType().defaultValue(0L);
    public static final ConfigOption<Long> FLINK_NETWORK_MEMORY_MIN =
            ConfigOptions.key("flink.network.memory.min").longType().defaultValue(0L);
    public static final ConfigOption<Long> FLINK_NETWORK_MEMORY_MAX =
            ConfigOptions.key("flink.network.memory.max").longType().defaultValue(0L);
    public static final ConfigOption<Float> FLINK_NETWORK_MEMORY_FRACTION =
            ConfigOptions.key("flink.network.memory.fraction").floatType().defaultValue(0f);

    public static final ConfigOption<String> ICEBERG_URI =
            ConfigOptions.key("iceberg.uri").stringType().noDefaultValue();
    public static final ConfigOption<String> ICEBERG_SOURCE_CATALOG =
            ConfigOptions.key("iceberg.source.catalog").stringType().noDefaultValue();
    public static final ConfigOption<String> ICEBERG_SOURCE_DATABASE =
            ConfigOptions.key("iceberg.source.database").stringType().noDefaultValue();

    public static final ConfigOption<String> MIXED_ICEBERG_CATALOG =
            ConfigOptions.key("mixed.iceberg.catalog").stringType().noDefaultValue();
    public static final ConfigOption<String> MIXED_ICEBERG_DATABASE =
            ConfigOptions.key("mixed.iceberg.database").stringType().noDefaultValue();
    public static final ConfigOption<String> KAFKA_ADDRESS =
            ConfigOptions.key("kafka.address").stringType().noDefaultValue();
    public static final ConfigOption<String> KAFKA_TOPIC =
            ConfigOptions.key("kafka.topic").stringType().noDefaultValue();
    public static final ConfigOption<String> ICEBERG_WAREHOUSE =
            ConfigOptions.key("iceberg.warehouse").stringType().noDefaultValue();
    public static final ConfigOption<Boolean> ICEBERG_WRITE_UPSERT_ENABLE =
            ConfigOptions.key("iceberg.write.upsert.enable").booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> ICEBERG_LIMITED =
            ConfigOptions.key("limiter.limited").booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> FLINK_DISABLE_OPERATING_CHAIN =
            ConfigOptions.key("flink.disable.operating.chain").booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> ICEBERG_TEST_TOTAL_SIZE =
            ConfigOptions.key("limiter.test.total.size").booleanType().defaultValue(false);
    public static final ConfigOption<Integer> ICEBERG_SINK_PARALLELISM =
            ConfigOptions.key("iceberg.sink.parallelism").intType().defaultValue(4);
    public static final ConfigOption<String> ICEBERG_LIMIT_RATE =
            ConfigOptions.key("limiter.limit.rate").stringType().noDefaultValue();
    public static final ConfigOption<String> HADOOP_USER_NAME =
            ConfigOptions.key("hadoop.user.name").stringType().defaultValue("");
    public static final ConfigOption<Long> FLINK_CHECKPOINT_INTERVAL =
            ConfigOptions.key("flink.checkpoint.interval").longType().defaultValue(60 * 1000L);
    public static final ConfigOption<String> FLINK_CHECKPOINT_TIMEOUT =
            ConfigOptions.key("flink.checkpoint.timeout").stringType().defaultValue("1200*1000");
    public static final ConfigOption<Integer> FLINK_CHUNK_SIZE =
            ConfigOptions.key("flink.chunk.size").intType().defaultValue(8096);

}
