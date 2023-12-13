package com.cdc.source;

import com.cdc.params.BaseParameters;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public interface Source {
    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv);
}
