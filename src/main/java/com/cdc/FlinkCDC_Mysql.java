package com.cdc;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.cdc.sink.Sink;
import com.cdc.source.Source;
import com.cdc.util.LimitRateUtil;
import com.cdc.util.ParameterUtil;
import org.apache.commons.cli.*;
import org.apache.flink.configuration.*;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("all")
public class FlinkCDC_Mysql {
    public static final String EDUARD_CONF_FILENAME = "ingestion-conf.yaml";
    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;
    private static BaseParameters baseParameters;
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDC_Mysql.class);

    public static void main(String[] args) throws Exception {


        org.apache.log4j.LogManager.resetConfiguration();
        String conf = System.getProperty("user.dir") + "/conf/";
        org.apache.log4j.PropertyConfigurator.configure(conf + "log4j2.properties");
        String[] params = parseParams(args);
        String source = params[0];
        String sink = params[1];
        String confDir = params[2];
        Map<String, String> props = new HashMap<>();
        Configuration configuration = loadConfiguration(confDir, props);

        baseParameters = new BaseParameters(configuration);
        MetaData.getMetaData(getSourceProperties(baseParameters));
        if (!baseParameters.getHadoopUserName().isEmpty()) {
            System.setProperty("HADOOP_USER_NAME", baseParameters.getHadoopUserName());
        }
        env = StreamExecutionEnvironment.getExecutionEnvironment(setFlinkConf(baseParameters.getFlinkPort()));
        env.setStateBackend(new RocksDBStateBackend("file:///mnt/dfs/1/benchmark-ingestion", true));
        env.getCheckpointConfig().setCheckpointInterval(baseParameters.getFlinkCheckpointInterval());
        env.enableCheckpointing(baseParameters.getFlinkCheckpointInterval());
        env.getCheckpointConfig().setCheckpointTimeout(baseParameters.getFlinkCheckpointTimeout());
        env.setParallelism(baseParameters.getIcebergSinkParallelism());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        tableEnv = StreamTableEnvironment.create(env);
        if (baseParameters.getDisableChain()) env.disableOperatorChaining();
        if (baseParameters.getIcebergLimited()) LimitRateUtil.createRateLimiter(baseParameters.getIcebergLimit());
        StatementSet statementSet = tableEnv.createStatementSet();
        prepareSql(source, sink, statementSet);
        statementSet.execute();
        if (baseParameters.getIcebergTestTotalSize()) LimitRateUtil.testTotalSize();
    }

    public static void prepareSql(String source, String sink, StatementSet statementSet) throws InstantiationException, IllegalAccessException {
        ((Source) ParameterUtil.getSourceClass(source).newInstance()).createAllTable(baseParameters, tableEnv);
        Sink sinker = ((Sink) ParameterUtil.getSinkClass(sink).newInstance());
        sinker.createAllTable(baseParameters, tableEnv);
        sinker.sink(source, statementSet, baseParameters, tableEnv);
    }

    private static Properties getSourceProperties(BaseParameters baseParameters) {
        Properties properties = new Properties();
        properties.put("url", "jdbc:mysql://" + baseParameters.getSourceHostName() + ":" + baseParameters.getSourcePort() + "/" + baseParameters.getSourceDatabaseName());
        properties.put("username", baseParameters.getSourceUserName());
        properties.put("password", baseParameters.getSourcePassword());
        properties.put("databaseName", baseParameters.getSourceDatabaseName());
        return properties;
    }

    private static Configuration loadConfiguration(final String configDir,
                                                   Map<String, String> props) {

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '" + configDir + "' (" +
                            confDirFile.getAbsolutePath() + ") does not describe an existing directory.");
        }

        // get Flink yaml configuration file
        final File yamlConfigFile = new File(confDirFile, EDUARD_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException("The Flink config file '" + yamlConfigFile + "' (" +
                    yamlConfigFile.getAbsolutePath() + ") does not exist.");
        }

        return loadYAMLResource(yamlConfigFile, props);
    }

    private static Configuration loadYAMLResource(File file, Map<String, String> props) {
        final Configuration config = new Configuration();

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath())))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    if (kv.length == 1) {
                        LOG.warn("Error while trying to split key and value in configuration file " +
                                EDUARD_CONF_FILENAME + ":" + lineNo + ": \"" + line + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn("Error after splitting key and value in configuration file " +
                                EDUARD_CONF_FILENAME + ":" + lineNo + ": \"" + line + "\"");
                        continue;
                    }

                    LOG.info("Loading configuration property: {}, {}", key, value);
                    config.setString(key, value);
                    props.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    private static String[] parseParams(String[] args) {
        Options options = new Options();
        Option source = Option.builder("source").required(true).hasArg()
                .argName("source").desc("source type").build();
        Option sink = Option.builder("sink").required(false).hasArg().argName("sink")
                .desc("sink type").build();

        Option confDir = Option.builder("confDir").required(true).hasArg()
                .argName("confDir").desc("Specify the database name of target confDir").build();


        options.addOption(source);
        options.addOption(sink);
        options.addOption(confDir);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        String[] params = new String[3];
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (cmd.hasOption("source")) {
            params[0] = cmd.getOptionValue("source");
        } else {
            throw new RuntimeException("parse Param 'source' fail");
        }
        if (cmd.hasOption("sink")) {
            params[1] = cmd.getOptionValue("sink");
        } else {
            throw new RuntimeException("parse Param 'sink' fail");
        }
        if (cmd.hasOption("confDir")) {
            params[2] = cmd.getOptionValue("confDir");
        } else {
            throw new RuntimeException("parse Param 'confDir' fail");
        }

        return params;
    }

    private static Configuration setFlinkConf(int restPort) {

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, restPort);
        configuration.setLong("table.exec.state.ttl", 7200000);
        configuration.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 7200000);
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, baseParameters.getFlinkNumTaskSlots());
        configuration.setString("execution.checkpointing.unaligned.forced", "true");
        if (!baseParameters.getFlinkNetworkMemoryMin().equals(0L))
            configuration.setString("taskmanager.memory.network.min", baseParameters.getFlinkNetworkMemoryMin().toString() + "m");
        if (!baseParameters.getFlinkNetworkMemoryMax().equals(0L))
            configuration.setString("taskmanager.memory.network.max", baseParameters.getFlinkNetworkMemoryMax().toString() + "m");
        if (!baseParameters.getFlinkNetworkMemoryFraction().equals(0f))
            configuration.setString("taskmanager.memory.network.fraction", baseParameters.getFlinkNetworkMemoryFraction().toString());

        return configuration;
    }

}
