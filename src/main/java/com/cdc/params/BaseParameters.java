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

package com.cdc.params;

import com.cdc.conf.BaseConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;


public class BaseParameters {

    public static String[] tables = {"customer", "district", "history", "item", "nation"
            , "new_order", "oorder", "order_line", "region", "stock", "supplier", "warehouse"};

    protected final Configuration eduardConfig;

    public BaseParameters(Configuration eduardConfig) {
        this.eduardConfig = Preconditions.checkNotNull(eduardConfig);
    }

    public String getSourceDatabaseName() {
        return eduardConfig.getString(BaseConfigOptions.SOURCE_DATABASE_NAME);
    }

    public String getSourceUserName() {
        return eduardConfig.getString(BaseConfigOptions.SOURCE_USERNAME);
    }

    public String getMode() {
        return eduardConfig.getString(BaseConfigOptions.SINK_MODE);
    }

    public String getIcebergSinkDatabase() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_SINK_DATABASE);
    }

    public String getIcebergSinkCatalog() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_SINK_CATALOG);
    }

    public String getBinlogFile() {
        return eduardConfig.getString(BaseConfigOptions.SINK_BINLOG_FILE);
    }

    public String getBinlogPos() {
        return eduardConfig.getString(BaseConfigOptions.SINK_BINLOG_POS);
    }

    public String getSourcePassword() {
        return eduardConfig.getString(BaseConfigOptions.SOURCE_PASSWORD);
    }

    public String getSourceHostName() {
        return eduardConfig.getString(BaseConfigOptions.SOURCE_HOSTNAME);
    }

    public String getSourcePort() {
        return eduardConfig.getString(BaseConfigOptions.SOURCE_PORT);
    }

    public Integer getIcebergLimit() {
        return calculateExpression(eduardConfig.getString(BaseConfigOptions.ICEBERG_LIMIT_RATE));
    }

    public Boolean getIcebergLimited() {
        return eduardConfig.getBoolean(BaseConfigOptions.ICEBERG_LIMITED);
    }

    public Boolean getDisableChain() {
        return eduardConfig.getBoolean(BaseConfigOptions.FLINK_DISABLE_OPERATING_CHAIN);
    }

    public int getSourceParallelism() {
        return eduardConfig.getInteger(BaseConfigOptions.SOURCE_PARALLELISM);
    }

    public String getHadoopUserName() {
        return eduardConfig.getString(BaseConfigOptions.HADOOP_USER_NAME);
    }

    public String getIcebergURI() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_URI);
    }

    public String getIcebergSourceCatalog() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_SOURCE_CATALOG);
    }

    public String getIcebergSourceDatabase() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_SOURCE_DATABASE);
    }

    public String getMixedIcebergCatalog() {
        return eduardConfig.getString(BaseConfigOptions.MIXED_ICEBERG_CATALOG);
    }

    public String getMixedIcebergDatabase() {
        return eduardConfig.getString(BaseConfigOptions.MIXED_ICEBERG_DATABASE);
    }

    public String getKafkaAddress() {
        return eduardConfig.getString(BaseConfigOptions.KAFKA_ADDRESS);
    }

    public String getKafkaTopic() {
        return eduardConfig.getString(BaseConfigOptions.KAFKA_TOPIC);
    }

    public String getIcebergWarehouse() {
        return eduardConfig.getString(BaseConfigOptions.ICEBERG_WAREHOUSE);
    }

    public Integer getIcebergSinkParallelism() {
        return eduardConfig.getInteger(BaseConfigOptions.ICEBERG_SINK_PARALLELISM);
    }

    public Integer getFlinkPort() {
        return eduardConfig.getInteger(BaseConfigOptions.FLINK_PORT);
    }

    public Integer getFlinkNumTaskSlots() {
        return eduardConfig.getInteger(BaseConfigOptions.FLINK_NUM_TASK_SLOTS);
    }

    public boolean getIcebergWriteUpsertEnable() {
        return eduardConfig.getBoolean(BaseConfigOptions.ICEBERG_WRITE_UPSERT_ENABLE);
    }

    public boolean getIcebergTestTotalSize() {
        return eduardConfig.getBoolean(BaseConfigOptions.ICEBERG_TEST_TOTAL_SIZE);
    }

    public Long getFlinkCheckpointInterval() {
        return eduardConfig.getLong(BaseConfigOptions.FLINK_CHECKPOINT_INTERVAL);
    }

    public Long getFlinkCheckpointTimeout() {
        return new Long(calculateExpression(eduardConfig.getString(BaseConfigOptions.FLINK_CHECKPOINT_TIMEOUT)));
    }

    public Long getIcebergRewriteFileSize() {
        return new Long(eduardConfig.getLong(BaseConfigOptions.ICEBERG_REWRITE_FILE_SIZE_BYTE));
    }


    public Long getFlinkNetworkMemoryMin() {
        return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_NETWORK_MEMORY_MIN));
    }

    public Long getFlinkNetworkMemoryMax() {
        return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_NETWORK_MEMORY_MAX));
    }

    public String getOptimizeGroupName() {
        return eduardConfig.getString(BaseConfigOptions.ARCTIC_OPTIMIZE_GROUP_NAME);
    }


    public Float getFlinkNetworkMemoryFraction() {
        return new Float(eduardConfig.getFloat(BaseConfigOptions.FLINK_NETWORK_MEMORY_FRACTION));
    }

    public Integer getFlinkChunkSize() {
        return eduardConfig.getInteger(BaseConfigOptions.FLINK_CHUNK_SIZE);
    }

    public Integer calculateExpression(String rateStr) {
        String[] split = rateStr.split("\\*");
        int result = 1;
        for (String numStr : split) {
            result *= Integer.parseInt(numStr);
        }
        return result;
    }
}
