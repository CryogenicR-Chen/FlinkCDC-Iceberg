package com.cdc;

import com.cdc.params.BaseParameters;
import com.cdc.params.MetaData;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.flink.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;
public class FlinkCDC_Mysql {
    static String[] tables = {"customer", "district","history", "item", "nation"
            ,"new_order","oorder","order_line","region","stock","supplier","warehouse"};
//    static String[] tables ={"customer"};

    public static final String EDUARD_CONF_FILENAME = "ingestion-conf.yaml";
    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;

    private static BaseParameters baseParameters;
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDC_Mysql.class);
    private static RateLimiter rateLimiter;

    private static AtomicLong allTableSize = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        org.apache.log4j.LogManager.resetConfiguration();
        String conf = System.getProperty("user.dir") + "/conf/";
        org.apache.log4j.PropertyConfigurator.configure(conf + "log4j2.properties");
        String[] params = parseParams(args);
        String databaseName = params[0];
        Boolean sinkKafka = Boolean.parseBoolean(params[1]);
        String kafkaTopic = params[2];
        String confDir = params[3];
        Map<String, String> props = new HashMap<>();
        Configuration configuration = null;
        configuration = loadConfiguration(confDir, props);
        baseParameters = new BaseParameters(configuration);
        if (!baseParameters.getHadoopUserName().isEmpty()) {
            System.setProperty("HADOOP_USER_NAME", baseParameters.getHadoopUserName());
        }
        env = StreamExecutionEnvironment.getExecutionEnvironment(setFlinkConf(baseParameters.getFlinkPort()));
//        env.disableOperatorChaining();
        env.setStateBackend(new FsStateBackend("file:///mnt/dfs/1/benchmark-ingestion"));
            env.getCheckpointConfig().setCheckpointInterval(baseParameters.getFlinkCheckpointInterval());
        env.getCheckpointConfig().setCheckpointTimeout(baseParameters.getFlinkCheckpointTimeout());
        env.setParallelism(baseParameters.getIcebergSinkParallelism());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        tableEnv = StreamTableEnvironment.create(env);
        if(baseParameters.getIcebergLimited())rateLimiter = RateLimiter.create(baseParameters.getIcebergLimit());
        StatementSet statementSet = tableEnv.createStatementSet();;
        if(sinkKafka){
            sinkKafka(kafkaTopic,statementSet);
        }else{
            sinkIcebergFull(databaseName,kafkaTopic,statementSet);
//            sinkIcebergIncremental(databaseName,statementSet);
        }
        statementSet.execute();

        if(baseParameters.getIcebergTestTotalSize()){
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            Runnable task = () -> {
                LOG.info("The total size is: " + allTableSize.get() + "byte.");
            };
            executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);
        }
    }
    public static void sinkIcebergIncremental(String databaseName,StatementSet statementSet){


        String catalogName = "my_catalog";
        for(String table : tables){
            Map<String,List<String>> mysqlSchema = MetaData.getMetaData2(getSourceProperties(baseParameters),table);
            createMysqlTable(table,mysqlSchema,baseParameters,"latest-offset");
            Table mysqlTable = tableEnv.from(table);

            if(baseParameters.getIcebergLimited()) limitRate(mysqlTable,table+"mysql",mysqlSchema);
            String tableName = baseParameters.getIcebergLimited() ? "temp_view_"+table+"mysql" : table;

            String sql = "INSERT INTO "+catalogName+"."+databaseName+"."+table+"_iceberg SELECT * FROM "+tableName;
            statementSet.addInsertSql(sql);
        }


    }
    public static void createMysqlTable(String table, Map<String,List<String>> mysqlSchema, BaseParameters baseParameters, String mode){
        List<String> columns = mysqlSchema.get("columns");
        List<String> primaryKeys = mysqlSchema.get("primaryKeys");
        String mysqlCreateTableSql = "CREATE TABLE " + table + " (\n";
        for (String str : columns) {
            mysqlCreateTableSql += "  " + str + ",\n";
        }
        String primaryKeyString = String.join(", ", primaryKeys);
        mysqlCreateTableSql += "  PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n) WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '"+baseParameters.getSourceHostName()+"',\n" +
                "  'port' = '"+baseParameters.getSourcePort()+"',\n" +
                "  'username' = '"+baseParameters.getSourceUserName()+"',\n" +
                "  'password' = '"+baseParameters.getSourcePassword()+"',\n" +
                "  'database-name' = '"+baseParameters.getSourceDatabaseName()+"',\n" +
                "  'table-name' = '" + table + "',\n"+
                "  'scan.startup.mode' = '" + mode +"',\n" +
                "  'scan.incremental.snapshot.chunk.size' = '"+baseParameters.getFlinkChunkSize()+"',\n" +
                "  'debezium.timezone' = 'Asia/Shanghai'\n" +
                ")";
        tableEnv.executeSql(mysqlCreateTableSql);
    }
    public static void sinkIcebergFull(String databaseName, String kafkaTopic,StatementSet statementSet){


        String catalogName = "my_catalog";
        tableEnv.executeSql("CREATE CATALOG " + catalogName + " WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'uri' = '"+baseParameters.getIcebergURI()+"',\n" +
                "  'warehouse' = 'hdfs://hz11-trino-arctic-0.jd.163.org:8020"+baseParameters.getIcebergWarehouse()+"'\n" +
                ")");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS "+catalogName+"."+databaseName);
        for(String table : tables){
            Map<String,List<String>> mysqlSchema = MetaData.getMetaData2(getSourceProperties(baseParameters),table);
            List<String> columns = mysqlSchema.get("columns");
            List<String> primaryKeys = mysqlSchema.get("primaryKeys");
            String primaryKeyString = String.join(", ", primaryKeys);
            String kafkaCreateTableSql = "CREATE TABLE kafka_" + table + " (\n";
            for (int i = 0; i < columns.size(); i++) {
                kafkaCreateTableSql += "  " + columns.get(i);
                if (i < columns.size() - 1) {
                    kafkaCreateTableSql += ",\n";
                } else {
                    kafkaCreateTableSql += ",\n" +
                            "   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n";
                }
            }

            kafkaCreateTableSql += ") WITH (\n" +
                    "  'connector' = 'kafka',\n" +
                    "  'topic' = '"+kafkaTopic + table + "',\n" +
                    "  'scan.startup.mode' = 'earliest-offset',\n" +
                    "  'properties.group.id' = 'flinkcdc-group',\n" +
                    "  'properties.bootstrap.servers' = '"+baseParameters.getKafkaAddress()+"',\n" +
                    " 'format' = 'canal-json'\n" +
                    ")";
            tableEnv.executeSql(kafkaCreateTableSql);
            Table kafkaTable = tableEnv.from("kafka_" + table);

            if(baseParameters.getIcebergLimited()) limitRate(kafkaTable,table,mysqlSchema);
            String tableName = baseParameters.getIcebergLimited() ? "temp_view_"+table : "kafka_" + table;

            String dropTableSql = "DROP TABLE IF EXISTS " + catalogName + "." + databaseName + "." + tableName+"_iceberg";
            tableEnv.executeSql(dropTableSql);
            String icebergCreateTableSql = "CREATE TABLE IF NOT EXISTS " +catalogName+"."+databaseName+"."+table+ "_iceberg (\n";
            for (int i = 0; i < columns.size(); i++) {
                icebergCreateTableSql += "  " + columns.get(i);
                if (i < columns.size() - 1) {
                    icebergCreateTableSql += ",\n";
                } else {
//                    icebergCreateTableSql += ",\n" +
//                            "   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n) WITH (\n" +
//                            "  'write.upsert.enable' = '"+ baseParameters.getIcebergWriteUpsertEnable() +"'\n" +
//                            ")";
//                    icebergCreateTableSql += "\n) " +
//                            "WITH (\n" +
////                            "  'format-version' = '2',\n"+
//                            "  'write.upsert.enabled' = '"+ baseParameters.getIcebergWriteUpsertEnable() +"'\n" +
//                            ")";
                    String optimizer = baseParameters.getOptimizeGroupName().equals("default") ? "" : " 'self-optimizing.enabled' = 'true',\n 'self-optimizing.group' = '"+ baseParameters.getOptimizeGroupName() + "',\n";
                    String rewriteFileSize = baseParameters.getIcebergRewriteFileSize().equals(0L) ? "" : " 'write.target-file-size-bytes' = '" + baseParameters.getIcebergRewriteFileSize() + "',\n";
                    icebergCreateTableSql += ",\n" +
                            "   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n) " +
                            "WITH (\n" +
                            optimizer + rewriteFileSize +
                            "  'format-version' = '2',\n"+
                            "  'write.upsert.enabled' = '"+ baseParameters.getIcebergWriteUpsertEnable() +"'\n" +
                            ")";
                }

            }
            tableEnv.executeSql(icebergCreateTableSql);

            String sql = "INSERT INTO "+catalogName+"."+databaseName+"."+table+"_iceberg SELECT * FROM "+tableName;
            statementSet.addInsertSql(sql);
        }

    }

    public static void limitRate(Table table, String tableName, Map<String,List<String>> mysqlSchema){
        List<String> columns = mysqlSchema.get("columns");
        List<String> primaryKeys = mysqlSchema.get("primaryKeys");
        Integer totalSize = Integer.parseInt(mysqlSchema.get("totalSize").get(0));
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
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table,schema);

        DataStream<Row> processedStream = dataStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) {
                allTableSize.addAndGet(totalSize);
                rateLimiter.acquire(totalSize);
                return value;
            }
        },dataStream.getType());
        tableEnv.createTemporaryView("temp_view_"+tableName, processedStream);
    }
    public static void sinkKafka(String kafkaTopic, StatementSet statementSet){

        for (String table : tables) {
            Map<String,List<String>> mysqlSchema = MetaData.getMetaData2(getSourceProperties(baseParameters),table);
            List<String> columns = mysqlSchema.get("columns");
            List<String> primaryKeys = mysqlSchema.get("primaryKeys");
            String primaryKeyString = String.join(", ", primaryKeys);
            createMysqlTable(table,mysqlSchema,baseParameters,"initial");
            String kafkaCreateTableSql = "CREATE TABLE kafka_" + table + " (\n";
            for (int i = 0; i < columns.size(); i++) {
                kafkaCreateTableSql += "  " + columns.get(i);
                if (i < columns.size() - 1) {
                    kafkaCreateTableSql += ",\n";
                } else {
                        kafkaCreateTableSql += ",\n" +
                            "   PRIMARY KEY (" + primaryKeyString + ") NOT ENFORCED\n";
                }
            }
            kafkaCreateTableSql += ") WITH (\n" +
                    "  'connector' = 'kafka',\n" +
                    "  'topic' = '"+ kafkaTopic + table + "',\n" +
                    "  'scan.startup.mode' = 'earliest-offset',\n" +
                    "  'properties.bootstrap.servers' = '"+baseParameters.getKafkaAddress()+"',\n" +
                    " 'format' = 'canal-json'\n" +
                    ")";

            tableEnv.executeSql(kafkaCreateTableSql);
            Table table1 = tableEnv.from(table);
            statementSet.addInsert("kafka_"+table, table1);
        }
    }
    private static Properties getSourceProperties(BaseParameters baseParameters){
        Properties properties = new Properties();
        properties.put("url","jdbc:mysql://"+baseParameters.getSourceHostName()+":"+baseParameters.getSourcePort()+"/"+baseParameters.getSourceDatabaseName());
        properties.put("username",baseParameters.getSourceUserName());
        properties.put("password",baseParameters.getSourcePassword());
        properties.put("databaseName",baseParameters.getSourceDatabaseName());
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
        Option databaseName = Option.builder("databaseName").required(true).hasArg()
                .argName("sinkDatabase").desc("Specify the database name of target database").build();
        Option sinkKafka = Option.builder("sinkKafka").required(false).hasArg().argName("sinkKafka")
                .desc("sinkKafka or sinkIceberg").build();
        Option kafkaTopic= Option.builder("kafkaTopic").required(true).hasArg()
                .argName("kafkaName").desc("Specify the database name of target kafkaTopic").build();

        Option confDir= Option.builder("confDir").required(true).hasArg()
                .argName("confDir").desc("Specify the database name of target confDir").build();


        options.addOption(databaseName);
        options.addOption(sinkKafka);
        options.addOption(kafkaTopic);
        options.addOption(confDir);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        String[] params = new String[4];
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (cmd.hasOption("databaseName")) {
            params[0] = cmd.getOptionValue("databaseName");
        } else {
            throw new RuntimeException("parse Param 'databaseName' fail");
        }
        if (cmd.hasOption("sinkKafka")) {
            params[1] = cmd.getOptionValue("sinkKafka");
        } else {
            throw new RuntimeException("parse Param 'sinkKafka' fail");
        }
        if (cmd.hasOption("kafkaTopic")) {
            params[2] = cmd.getOptionValue("kafkaTopic");
        } else {
            throw new RuntimeException("parse Param 'kafkaTopic' fail");
        }
        if (cmd.hasOption("confDir")) {
            params[3] = cmd.getOptionValue("confDir");
        } else {
            throw new RuntimeException("parse Param 'confDir' fail");
        }

        return params;
    }

    private static Configuration setFlinkConf(int restPort) {

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, restPort);
        if(!baseParameters.getFlinkManagedMemorySize().equals(0L))configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(baseParameters.getFlinkManagedMemorySize()));
        if(!baseParameters.getFlinkNetworkMemoryMin().equals(0L))configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(baseParameters.getFlinkNetworkMemoryMin()));
        if(!baseParameters.getFlinkNetworkMemoryMax().equals(0L))configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(baseParameters.getFlinkNetworkMemoryMax()));
        if(!baseParameters.getFlinkTaskHeapMemory().equals(0L))configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(baseParameters.getFlinkTaskHeapMemory()));
        if(!baseParameters.getFlinkFrameworkHeapMemory().equals(0L))configuration.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ofMebiBytes(baseParameters.getFlinkFrameworkHeapMemory()));
        if(!baseParameters.getFlinkManagedMemoryFraction().equals(0f))configuration.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, baseParameters.getFlinkManagedMemoryFraction());
        if(!baseParameters.getFlinkNetworkMemoryFraction().equals(0f))configuration.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, baseParameters.getFlinkNetworkMemoryFraction());

        if(!baseParameters.getFlinkHeartBeatTimeout().equals(0L))

            configuration.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, baseParameters.getFlinkHeartBeatTimeout());
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, baseParameters.getFlinkNumTaskSlots());
        configuration.setString("execution.checkpointing.unaligned.forced", "true");
        return configuration;
    }

}
