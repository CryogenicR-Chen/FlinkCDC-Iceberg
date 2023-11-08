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

  protected final Configuration eduardConfig;

  public BaseParameters(Configuration eduardConfig) {
    this.eduardConfig = Preconditions.checkNotNull(eduardConfig);
  }

  public Configuration getEduardConfig() {
    return eduardConfig;
  }

  public String getSourceType() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_TYPE);
  }

  public String getSourceDatabaseName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_DATABASE_NAME);
  }

  public String getSourceUserName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_USERNAME);
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

  public String getSourceTableName() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_TABLE_NAME);
  }

  public String getSourceServerTimeZone() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_SERVER_TIME_ZONE);
  }

  public String getSourceScanStartupMode() {
    return eduardConfig.getString(BaseConfigOptions.SOURCE_SCAN_STARTUP_MODE);
  }

  public int getSourceParallelism() {
    return eduardConfig.getInteger(BaseConfigOptions.SOURCE_PARALLELISM);
  }

  public String getHadoopUserName() {
    return eduardConfig.getString(BaseConfigOptions.HADOOP_USER_NAME);
  }

  public String getIcebergURI() {return  eduardConfig.getString(BaseConfigOptions.ICEBERG_URI);}
  public String getKafkaAddress() {return  eduardConfig.getString(BaseConfigOptions.KAFKA_ADDRESS);}
  public String getIcebergWarehouse() {return  eduardConfig.getString(BaseConfigOptions.ICEBERG_WAREHOUSE);}
  public Integer getIcebergSinkParallelism() {return  eduardConfig.getInteger(BaseConfigOptions.ICEBERG_SINK_PARALLELISM);}
  public Integer getFlinkPort() {return  eduardConfig.getInteger(BaseConfigOptions.FLINK_PORT);}
  public Integer getFlinkNumTaskSlots() {return  eduardConfig.getInteger(BaseConfigOptions.FLINK_NUM_TASK_SLOTS);}
  public boolean getIcebergWriteUpsertEnable() {return eduardConfig.getBoolean(BaseConfigOptions.ICEBERG_WRITE_UPSERT_ENABLE);}
  public boolean getIcebergTestTotalSize() {return eduardConfig.getBoolean(BaseConfigOptions.ICEBERG_TEST_TOTAL_SIZE);}
  public Long getFlinkCheckpointInterval() {return eduardConfig.getLong(BaseConfigOptions.FLINK_CHECKPOINT_INTERVAL);}
  public Long getFlinkCheckpointTimeout() {
    return new Long(calculateExpression(eduardConfig.getString(BaseConfigOptions.FLINK_CHECKPOINT_TIMEOUT)));}
 public Long getFlinkTaskHeapMemory() {
    return  new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_TASK_HEAP_MEMORY));}
  public Long getIcebergRewriteFileSize() {
    return  new Long(eduardConfig.getLong(BaseConfigOptions.ICEBERG_REWRITE_FILE_SIZE_BYTE));}
  public Long getFlinkFrameworkHeapMemory() {
    return  new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_FRAMEWORK_HEAP_MEMORY));}
 public Long getFlinkManagedMemorySize() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_MANAGED_MEMORY_SIZE));}
 public Long getFlinkManagedMemoryMax() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_MANAGED_MEMORY_MAX));}
public Long getFlinkNetworkMemoryMin() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_NETWORK_MEMORY_MIN));}
public Long getFlinkNetworkMemoryMax() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_NETWORK_MEMORY_MAX));}
  public String getOptimizeGroupName() {
    return eduardConfig.getString(BaseConfigOptions.ARCTIC_OPTIMIZE_GROUP_NAME);
  }
public Long getFlinkManagedMemoryMin() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_MANAGED_MEMORY_MIN));}
  public Long getFlinkHeartBeatTimeout() {
    return new Long(eduardConfig.getLong(BaseConfigOptions.FLINK_HEARTBEAT_TIMEOUT));}
public Float getFlinkManagedMemoryFraction() {
    return new Float(eduardConfig.getFloat(BaseConfigOptions.FLINK_MANAGED_MEMORY_FRACTION));}
public Float getFlinkNetworkMemoryFraction() {
    return new Float(eduardConfig.getFloat(BaseConfigOptions.FLINK_NETWORK_MEMORY_FRACTION));}
  public String getKafkaBootstrapServers() {return  eduardConfig.getString(BaseConfigOptions.KAFKA_BOOTSTRAP_SERVERS);}

  public Integer getFlinkChunkSize(){return eduardConfig.getInteger(BaseConfigOptions.FLINK_CHUNK_SIZE);}
  public Integer calculateExpression(String rateStr) {
    String[] split = rateStr.split("\\*");
    int result = 1;
    for (String numStr : split) {
      result *= Integer.parseInt(numStr);
    }
    return result;
  }
}
