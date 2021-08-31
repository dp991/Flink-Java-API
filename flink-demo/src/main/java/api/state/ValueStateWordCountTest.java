package api.state;

import api.state.entity.ValueStateRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zdp
 * @description wordcount by value state
 * @email 13221018869@189.cn
 * @date 2021/5/28 15:07
 */
public class ValueStateWordCountTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://fk-1:9000/backhand"));

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

        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                Arrays.stream(split).forEach(x -> collector.collect(new Tuple2<>(x, 1)));
            }
        }).keyBy(x -> x.f0).map(new ValueStateRichFunction()).print();

        env.execute();
    }
}
