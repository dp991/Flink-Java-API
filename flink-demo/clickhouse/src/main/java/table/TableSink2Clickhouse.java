package table;


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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TableSink2Clickhouse {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定checkpoint的触发间隔,必须设置checkpoint保存路径，默认是8s
        env.enableCheckpointing(10000L);
        // 默认的CheckpointingMode就是EXACTLY_ONCE，也可以指定为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //"hdfs://fk-1:9000/backhand"
        env.setStateBackend(new FsStateBackend("hdfs://10.50.51.46:8020/flink/backhand"));
//        env.setStateBackend(new FsStateBackend("file:///usr/local/apps/checkpoints"));

        //任务重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(6, Time.seconds(5))
        );

        // 程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.50.51.72:9092,10.50.51.79:9092,10.50.51.80:9092");

        //设置消费者组
        properties.setProperty("group.id", "flink-group1");


        properties.setProperty("auto.offset.reset", "earliest");


        //消费多个topic
        List<String> topics = new ArrayList<>();

        topics.add("cloud_switch_msg_type_6");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Msg> MsgStream = stream.flatMap(new FlatMapFunction<String, Msg>() {
            @Override
            public void flatMap(String s, Collector<Msg> collector) throws Exception {
                List<Msg> msgs = Helper.parseFromJSONString(s);
                if (msgs.size() != 0) {
                    msgs.stream().forEach(m -> {
                        collector.collect(m);
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

        String sql = "insert into cloud_switch_msg.cloud_switch_msg_type_6 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        MsgStream.addSink(JdbcSink.sink(sql, new CKSinkBuilder(), builder.build(), connection.build())).name("clickhouse-msg-type6");

        env.execute("kafka-flink-table-clickhouse 1p type6");
    }
}
