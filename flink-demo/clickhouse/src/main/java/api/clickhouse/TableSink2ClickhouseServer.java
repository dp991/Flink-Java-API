package api.clickhouse;

import api.clickhouse.domain.CKSinkBuilder;
import api.clickhouse.domain.TrunkFlowDto;
import api.clickhouse.utils.Helper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TableSink2ClickhouseServer {


    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度与slot
        env.setParallelism(1);
        //指定checkpoint的触发间隔,必须设置checkpoint保存路径
        env.enableCheckpointing(5000);
        // 默认的CheckpointingMode就是EXACTLY_ONCE，也可以指定为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //"hdfs://fk-1:9000/backhand"
        env.setStateBackend(new FsStateBackend("hdfs://10.50.51.46:8020/flink/backhand"));
        //本地测试使用下面的地址
//        env.setStateBackend(new FsStateBackend("file:///usr/local/apps/checkpoints"));

//        RocksDBStateBackend backend = new RocksDBStateBackend(filebackend, true);

        // 程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //任务重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(6, Time.seconds(5))
        );

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.50.51.72:9092,10.50.51.79:9092,10.50.51.80:9092");

        //设置消费者组
        properties.setProperty("group.id", "flink-group");

        //消费的三种方式，默认是latest
        //earliest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none： topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常

        properties.setProperty("auto.offset.reset", "earliest");

//        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
//        DataStreamSource stream = env.addSource(kafkaConsumer).setParallelism(2);


        //消费多个topic
        List<String> topics = new ArrayList<>();

        topics.add("cloud_switch_msg_type_5");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties));

        final AtomicInteger count = new AtomicInteger(0);

        SingleOutputStreamOperator<TrunkFlowDto> rsStream = stream.flatMap(new FlatMapFunction<String, TrunkFlowDto>() {
            @Override
            public void flatMap(String s, Collector<TrunkFlowDto> collector) throws Exception {
                List<TrunkFlowDto> list = Helper.string2Dto(s);
                if (list.size() != 0) {
                    list.stream().forEach(dto -> {
                        collector.collect(dto);
                    });
                }
            }
        });

        JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(10);

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connection = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        connection.withUrl("jdbc:clickhouse://10.50.51.46:8123")
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .withUsername("default")
                .withPassword("123456");

        String sql = "insert into cloud_switch_msg.cloud_switch_msg_type_5 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //sink
        rsStream.addSink(JdbcSink.sink(sql, new CKSinkBuilder(), builder.build(), connection.build())).name("clickhouse");

        env.execute("kafka-flink-table-clickhouse with 1 p type5");
    }

    public static void taskOne(){

    }
}
