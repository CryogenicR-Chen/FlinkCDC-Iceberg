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

package com.cdc.util;

import com.cdc.sink.SinkIceberg;
import com.cdc.sink.SinkKafka;
import com.cdc.sink.SinkMixedIceberg;
import com.cdc.source.SourceIceberg;
import com.cdc.source.SourceKafka;
import com.cdc.source.SourceMysql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ParameterUtil.class);

  public static Class<?> getSinkClass(String type) {

    switch (type) {
      case "mixed-iceberg":
        return SinkMixedIceberg.class;
      case "iceberg":
        return SinkIceberg.class;
      case "kafka":
        return SinkKafka.class;
      default:
        LOG.error("get type {} sink class fail", type);
        return null;
    }
  }
  public static Class<?> getSourceClass(String type) {

    switch (type) {
      case "iceberg":
        return SourceIceberg.class;
      case "kafka":
        return SourceKafka.class;
      case "mysql":
        return SourceMysql.class;
      default:
        LOG.error("get type {} source class fail", type);
        return null;
    }
  }
}
