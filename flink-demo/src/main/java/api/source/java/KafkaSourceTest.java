package api.source.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author zdp
 * @description 从kafka消费消息
 * @email 13221018869@189.cn
 * @date 2021/5/26 14:20
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定checkpoint的触发间隔
        env.enableCheckpointing(5000);
        // 默认的CheckpointingMode就是EXACTLY_ONCE，也可以指定为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.50.51.72:9092");

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
        topics.add("test");
        topics.add("words");
        topics.add("cloud_switch_msg_type_9");
        DataStream<String> streamBatch = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties));

        streamBatch.print();
        env.execute("comsumer start");
    }
}
