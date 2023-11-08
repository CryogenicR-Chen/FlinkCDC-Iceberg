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
  public static final ConfigOption<String> SOURCE_TYPE =
      ConfigOptions.key("source.type").stringType().noDefaultValue();

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

  public static final ConfigOption<String> SOURCE_TABLE_NAME =
      ConfigOptions.key("source.table.name").stringType().defaultValue("*");

  public static final ConfigOption<String> SOURCE_SCAN_STARTUP_MODE =
      ConfigOptions.key("source.scan.startup.mode").stringType().defaultValue("initial");

  public static final ConfigOption<String> SOURCE_SERVER_TIME_ZONE =
      ConfigOptions.key("source.server.timezone").stringType().defaultValue("");
  public static final ConfigOption<String> ARCTIC_OPTIMIZE_GROUP_NAME =
          ConfigOptions.key("arctic.optimize.group.name").stringType().defaultValue("default");
  public static final ConfigOption<Integer> SOURCE_PARALLELISM =
      ConfigOptions.key("source.parallelism").intType().defaultValue(4);
  public static final ConfigOption<Integer> FLINK_PORT =
      ConfigOptions.key("flink.port").intType().defaultValue(8081);
  public static final ConfigOption<Integer> FLINK_NUM_TASK_SLOTS =
          ConfigOptions.key("flink.num.task.slots").intType().defaultValue(4);
  public static final ConfigOption<Long> FLINK_MANAGED_MEMORY_SIZE =
          ConfigOptions.key("flink.managed.memory.size").longType().defaultValue(0L);
  public static final ConfigOption<Long> FLINK_TASK_HEAP_MEMORY =
          ConfigOptions.key("flink.task.heap.memory").longType().defaultValue(0L);
  public static final ConfigOption<Long> ICEBERG_REWRITE_FILE_SIZE_BYTE =
          ConfigOptions.key("write.target-file-size-bytes").longType().defaultValue(0L);
  public static final ConfigOption<Long> FLINK_FRAMEWORK_HEAP_MEMORY =
          ConfigOptions.key("flink.framework.heap.memory").longType().defaultValue(0L);
public static final ConfigOption<Long> FLINK_MANAGED_MEMORY_MAX =
          ConfigOptions.key("flink.managed.memory.max").longType().defaultValue(0L);
public static final ConfigOption<Long> FLINK_NETWORK_MEMORY_MIN =
          ConfigOptions.key("flink.network.memory.min").longType().defaultValue(0L);
public static final ConfigOption<Long> FLINK_NETWORK_MEMORY_MAX =
          ConfigOptions.key("flink.network.memory.max").longType().defaultValue(0L);
public static final ConfigOption<Long> FLINK_MANAGED_MEMORY_MIN =
          ConfigOptions.key("flink.managed.memory.min").longType().defaultValue(0L);
public static final ConfigOption<Long> FLINK_HEARTBEAT_TIMEOUT =
          ConfigOptions.key("flink.heartbeat.timeout").longType().defaultValue(0L);
public static final ConfigOption<Float> FLINK_MANAGED_MEMORY_FRACTION =
          ConfigOptions.key("flink.managed.memory.fraction").floatType().defaultValue(0f);
public static final ConfigOption<Float> FLINK_NETWORK_MEMORY_FRACTION =
          ConfigOptions.key("flink.network.memory.fraction").floatType().defaultValue(0f);

  public static final ConfigOption<String> ICEBERG_URI =
          ConfigOptions.key("iceberg.uri").stringType().noDefaultValue();
public static final ConfigOption<String> KAFKA_ADDRESS =
          ConfigOptions.key("kafka.address").stringType().noDefaultValue();
  public static final ConfigOption<String> ICEBERG_WAREHOUSE =
          ConfigOptions.key("iceberg.warehouse").stringType().noDefaultValue();
  public static final ConfigOption<Boolean> ICEBERG_WRITE_UPSERT_ENABLE =
          ConfigOptions.key("iceberg.write.upsert.enable").booleanType().defaultValue(false);
  public static final ConfigOption<Boolean> ICEBERG_LIMITED =
          ConfigOptions.key("iceberg.limited").booleanType().defaultValue(false);
  public static final ConfigOption<Boolean> ICEBERG_TEST_TOTAL_SIZE =
          ConfigOptions.key("iceberg.test.total.size").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> ICEBERG_SINK_PARALLELISM =
          ConfigOptions.key("iceberg.sink.parallelism").intType().defaultValue(4);
  public static final ConfigOption<String> ICEBERG_LIMIT_RATE =
          ConfigOptions.key("iceberg.limit.rate").stringType().noDefaultValue();
  public static final ConfigOption<String> HADOOP_USER_NAME =
      ConfigOptions.key("hadoop.user.name").stringType().defaultValue("");
  public static final ConfigOption<Long> FLINK_CHECKPOINT_INTERVAL =
          ConfigOptions.key("flink.checkpoint.interval").longType().defaultValue(60*1000L);
  public static final ConfigOption<String> FLINK_CHECKPOINT_TIMEOUT =
          ConfigOptions.key("flink.checkpoint.timeout").stringType().defaultValue("1200*1000");
  public static final ConfigOption<Integer> FLINK_CHUNK_SIZE =
          ConfigOptions.key("flink.chunk.size").intType().defaultValue(8096);
  public static final ConfigOption<String> KAFKA_BOOTSTRAP_SERVERS =
          ConfigOptions.key("kafka.bootstrap.servers").stringType().noDefaultValue();

}
