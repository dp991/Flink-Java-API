# 创建kafka数据源

```shell script
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-reader

bin/kafka-console-producer.sh --broker-list fk-1:9092 --topic kafka-reader

```

# clickhouse创建目标表
```shell script
CREATE TABLE kafka.target(
day Date,
level String,
message String
)ENGINE=TinyLog;

```

# kafka消费表
```shell script
 CREATE TABLE kafka.queue (
    timestamp DateTime,
    level String,
    message String
  )
  ENGINE = Kafka
SETTINGS kafka_broker_list = '192.168.191.3:9092',
       kafka_topic_list = 'kafka-reader',
	   kafka_row_delimiter = '\n',
       kafka_group_name = 'kafka-reader-group',
       kafka_format = 'JSONEachRow'
```

# 创建Materialized View（物化视图）传输数据
```shell script
 CREATE MATERIALIZED VIEW consumer TO target
 AS SELECT toDate(toDateTime(timestamp)) AS day, level,message
 FROM queue;
```
# 测试数据
```shell script
bin/kafka-console-producer.sh --broker-list fk-1:9092 --topic kafka-reader
{"timestamp":"2021-11-30 13:45:34","level":"11","message":"上测试"}
{"timestamp":"2021-12-25 12:45:34","level":"89","message":"写博客"}
{"timestamp":"2021-01-05 08:45:34","level":"02","message":"喝可乐"}
{"timestamp":"2021-09-12 18:45:34","level":"72","message":"下班，回家"}
{"timestamp":"2021-06-06 21:45:34","level":"11","message":"打游戏"}
{"timestamp":"2021-06-07 22:45:34","level":"11","message":"打游戏2"}

```