# FlinkCDC-Iceberg

## 参数格式
-source [type] -sink [type] -confDir conf <br>
source : mysql, iceberg, kafka <br>
sink : kafka, iceberg, mixed-iceberg
## 流程
1. Mysql to Kafka <br>
全量部分配置source.sink.mode: full  增量部分配置source.sink.mode: increment 和binlog位点 <br>
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source mysql -sink kafka -confDir conf > 2output.txt 2>&1 &

2. Kafka to Iceberg <br>
配置iceberg.sink.catalog: catalog2  iceberg.sink.database: db4570 <br>
配置iceberg.write.upsert.enable: true 开启upsert <br>
配置iceberg.optimize.group.name: bench_group 开启optimizer <br>
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source kafka -sink iceberg -confDir conf > 2output.txt 2>&1 &

3. Kafka to MixedIceberg <br>
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source kafka -sink mixed-iceberg -confDir conf > 2output.txt 2>&1 &

## 脚本&命令
### 回滚iceberg
在sloth-commerce-test1.jd.163.org中启动spark sql 然后执行12张表的回滚sql 
```
unset SPARK_HOME
unset SPARK_CONF_DIR
unset HADOOP_CONF_DIR
export SPARK_HOME=/home/arctic/spark/spark-3.3.2-bin-hadoop2 
export SPARK_CONF_DIR=/home/arctic/spark/spark-3.3.2-bin-hadoop2/conf
export YARN_CONF_DIR=/home/arctic/spark/conf
export HADOOP_CONF_DIR=/home/arctic/spark/conf
unset JAVA_HOME
export HADOOP_USER_NAME=sloth
${SPARK_HOME}/bin/spark-sql    \  --conf       spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg_catalog4=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog4.type=hive \
    --conf spark.sql.catalog.iceberg_catalog4.warehouse=thrift://hz11-trino-arctic-0.jd.163.org:9083

CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.customer_iceberg', 1847092692435509058);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.district_iceberg', 8922977245387808827);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.history_iceberg', 3293079549760470453);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.item_iceberg', 6292386753375876793);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.nation_iceberg', 6268883065396625097);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.new_order_iceberg', 1799703221994796562);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.oorder_iceberg', 8973209824978503846);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.order_line_iceberg', 8341896147052346481);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.region_iceberg', 4663524173198535819);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.stock_iceberg', 1849499181873741057);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.supplier_iceberg', 7149563024346994053);
CALL iceberg_catalog4.system.rollback_to_snapshot('db4551.warehouse_iceberg', 6969833962445230510);
```

在测基准的时候可能会需要回到后面的快照
```
CALL iceberg_catalog4.system.set_current_snapshot('db4551.customer_iceberg', 3685984477667245011);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.district_iceberg', 7254495687699382118);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.history_iceberg', 3930542970055817122);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.item_iceberg', 6292386753375876793);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.nation_iceberg', 6268883065396625097);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.new_order_iceberg', 6667032743996593681);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.oorder_iceberg', 8473427391720631830);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.order_line_iceberg', 1941622126754182383);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.region_iceberg', 4663524173198535819);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.stock_iceberg', 7330562848215019924);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.supplier_iceberg', 7149563024346994053);
CALL iceberg_catalog4.system.set_current_snapshot('db4551.warehouse_iceberg', 1001736428873731528);
```

### Kafka脚本
在sloth-commerce-test1.jd.163.org 目录`cd /mnt/dfs/1/kafka_2.12-2.7.1/bin`下有create.sh用于生成topic delete.sh用于删除topic size2.sh用于计算topics的大小。

### Trino 运行命令
在sloth-commerce-test1.jd.163.org <br>
这个Trino运行完会自动开启下一轮查询！
```
export HADOOP_USER_NAME=sloth
export JAVA_HOME=/home/hadoop/presto/trino/jdk17/jdk-17.0.6
export PATH=$(echo "$PATH" | sed -e 's/:\/usr\/easyops\/jdk8\/bin//')
export PATH=/home/hadoop/presto/trino/jdk17/jdk-17.0.6/bin:$PATH
export PATH=/home/hadoop/presto/trino/trino-server-406/bin:$PATH

cd /mnt/dfs/1/lakehouse-benchmark-21-SNAPSHOT/temp/lakehouse-benchmark-21-SNAPSHOT/
nohup java -Dtpcc_name_suffix=_iceberg -jar lakehouse-benchmark.jar -b chbenchmarkForTrino -c config/trino/trino_chbenchmark_config.xml --create=false --load=false --execute=true > output.txt 2>&1 &
```
### Benchmark
在sloth-commerce-test1.jd.163.org
```
export HADOOP_USER_NAME=sloth
export JAVA_HOME=/home/hadoop/presto/trino/jdk17/jdk-17.0.6
export PATH=$(echo "$PATH" | sed -e 's/:\/usr\/easyops\/jdk8\/bin//')
export PATH=/home/hadoop/presto/trino/jdk17/jdk-17.0.6/bin:$PATH
export PATH=/home/hadoop/presto/trino/trino-server-406/bin:$PATH

cd /mnt/dfs/1/lakehouse-benchmark-21-SNAPSHOT
nohup /home/arctic/jdk-17.0.3/bin/java -jar lakehouse-benchmark-suc.jar -b tpcc,chbenchmark -c config/mysql/sample_chbenchmark_config.xml --create=true --load=true > output.txt 2>&1 &
nohup java -jar lakehouse-benchmark-suc.jar -b tpcc,chbenchmark -c config/mysql/sample_chbenchmark_config.xml --execute=true -s 5 > output.txt 2>&1 &
```

### 统计文件情况的脚本monitor.py
在 `cd /home/arctic/chenjianghantest/chenjianghan/workdir/lakehouse-benchmark-ingestion/real` 目录下
运行可查看iceberg mixed-iceberg的表情况，记得改database和catalog，cookie会过期也需要改。 <br>
脚本定期查看Trino状态，Trino挂掉会触发并查看表情况。（现在Trino的port是错的，运行查看当前表情况，需要监控时改成对的）

### 启动Optimizer
在sloth-commerce-test1.jd.163.org <br>
 内存 =  -Dtaskmanager.memory.process.size=4430m * 并发 -p 3  + -Djobmanager.memory.process.size=1024m
```
export FLINK_CONF_DIR=/home/arctic/arctic-flink/flink-1.12.7/conf && export HADOOP_USER_NAME=sloth && export HADOOP_CONF_DIR=/home/hadoop/presto/trino-online-env && /home/arctic/flink-1.14.6/bin/flink run --target=yarn-per-job -Dtaskmanager.memory.process.size=4430m -Djobmanager.memory.process.size=1024m -Dtaskmanager.memory.managed.size=32m -c com.netease.arctic.optimizer.flink.FlinkOptimizer /home/arctic/wangtao3/optimizer-job.jar  -a thrift://10.196.98.26:18151 -p 3 -g bench_group -eds -dsp /tmp -msz 512
```
### 启动Spark Rewrite
在sloth-commerce-test1.jd.163.org <br>
内存  = --executor-memory 4505m *  --num-executors 5 * --executor-cores 1 <br>
-c iceberg_catalog4 设置catalog   -s db4551 设置库 <br>
-m rewrite 这个参数没用，写上就行 <br>
-f 1 rewrite循环频率，单位是秒 <br>
-a 12张表rewrite <br>
如果没有-a的话 -t table_name 参数可以指定rewrite某一张表 <br>
-p 的取值为0-5，分别对应了以下sql，根据你需要的sql选择 <br>
String.format("CALL %s.system.rewrite_data_files('%s.%s')", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','true'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','false'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5','rewrite-all','true'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all','true'))", CATALOG, DB, localTableName); <br>

```
unset SPARK_HOME
unset SPARK_CONF_DIR
unset HADOOP_CONF_DIR
export SPARK_HOME=/home/arctic/spark/spark-3.3.2-bin-hadoop2 
export SPARK_CONF_DIR=/home/arctic/spark/spark-3.3.2-bin-hadoop2/conf
export YARN_CONF_DIR=/home/arctic/spark/conf
export HADOOP_CONF_DIR=/home/arctic/spark/conf
cd /home/arctic/spark/spark-3.3.2-bin-hadoop2/examples/jars

/home/arctic/spark/spark-3.3.2-bin-hadoop2/bin/spark-submit  --master yarn --deploy-mode client --num-executors 5 --executor-memory 4505m --executor-cores 1 --class org.rewrite.SparkRewrite /home/arctic/spark/spark-3.3.2-bin-hadoop2/examples/jars/spark-rewrite-1.7-SNAPSHOT.jar -c iceberg_catalog4 -s db4551 -a -m rewrite -f 1 -p 5
```
### 收集Trino查询结果脚本
在sloth-commerce-test1.jd.163.org <br>
在get.py修改日期前缀（你要查哪天的数据）
```
/mnt/dfs/1/lakehouse-benchmark-21-SNAPSHOT/temp/lakehouse-benchmark-21-SNAPSHOT/results
python get.py
```

##  注意事项
0. 运行trino查询前，或者发现amoro看不到表时，在hz11 hdfs用户下执行`hdfs dfs -chmod -R 777  /user/test_warehouse/db4569
`开启权限！
1.定期清理Mysql的binlog ` PURGE BINARY LOGS TO 'mysql-bin.001800';` ！一般两个1000warehouse的库在加一些增量或者其他的库就会占满mysql datadir。<br>
2.Mysql binlog的存活时间是70天，有需要可以延长 <br>
3.全量数据生成完，先导到kafka，然后再去生成增量 <br>
4.长期运行Spark rewrite会导致Hdfs里的表变得非常大，一个库可能占3-5T的空间, 回滚到你想要保存的快照位置，然后开启table-expire.enabled去清理数据。<br>
5.全量数据损坏，可以考虑通过回滚然后从iceberg里导到另一个iceberg或者mixed-iceberg <br>
`nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source iceberg- sink iceberg -confDir conf > 2output.txt 2>&1 &`
这个流程如果数据量很大，source iceberg默认并发量很大考虑以下参数
```
flink.num.task.slots: 100
iceberg.sink.parallelism: 50
flink.network.memory.max: 940
flink.network.memory.min: 940
flink.network.memory.fraction: 0.1
```
	
	