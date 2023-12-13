package com.cdc.sink;

import com.cdc.params.BaseParameters;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public interface Sink {
    public void sink(String source, StatementSet statementSet, BaseParameters baseParameters, StreamTableEnvironment tableEnv);

    public void createAllTable(BaseParameters baseParameters, StreamTableEnvironment tableEnv);


}
