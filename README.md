# FlinkCDC-Iceberg

参数格式
-source [type] -sink [type] -confDir conf

1. Mysql to Kafka
全量部分配置source.sink.mode: full
增量部分配置source.sink.mode: increment 和binlog位点
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source mysql -sink kafka -confDir conf > 2output.txt 2>&1 &

2. Kafka to Iceberg
配置iceberg.sink.catalog: catalog2
   iceberg.sink.database: db4570
配置iceberg.write.upsert.enable: true 开启upsert
配置iceberg.optimize.group.name: bench_group 开启optimizer
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source kafka -sink iceberg -confDir conf > 2output.txt 2>&1 &

3. Kafka to MixedIceberg
nohup java -Xmx125G -Xms125G -jar flink-2.0.jar -source kafka -sink mixed-iceberg -confDir conf > 2output.txt 2>&1 &


