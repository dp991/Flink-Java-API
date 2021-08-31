package api.state;

import api.state.entity.ReducingStateMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zdp
 * @description 通过ReducingState实现自动wordcount
 * @email 13221018869@189.cn
 * @date 2021/6/3 9:19
 */
public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        //设置最多尝试重启10次
        //任务失败5秒后开始执行重启操作
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(5)));

        //5分钟内，最大失败10次（第10次错误发生时，程序退出），而且每次失败10秒后再尝试重启
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        10,//第10次异常后，不会重启
                        Time.minutes(5),//计算失败次数的时间范围：5分钟
                        Time.seconds(10))//失败10秒后再尝试重启
        );


//        env.setStateBackend(new FsStateBackend("hdfs://fk-1:9000/out"));

        //kafka prop
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "fk-1:9092");
        prop.setProperty("group.id", "flink_consumer");
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("enable.auto.commit", "true");

        ArrayList<String> topics = new ArrayList<>();
        topics.add("words");

        //flink link to kafka
        DataStreamSource<String> lines = env.addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop));

//        lines.print();

        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(" ");
                Arrays.stream(strings).forEach(word -> collector.collect(new Tuple2<>(word, 1)));
            }
        }).keyBy(tuple2 -> tuple2.f0).map(new ReducingStateMapFunction()).print();

        env.execute();
    }
}
