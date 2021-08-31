package api.source.java;

import api.source.java.serialiable.MessageDeSerializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author zdp
 * @description 自定义kafka消费的序列化类
 * @email 13221018869@189.cn
 * @date 2021/5/26 18:46
 */
public class UdfKafkaSourceTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //自定义checkpoin出发时间
        env.enableCheckpointing(5000);
        //默认的CheckpointingMode就是EXACTLY_ONCE，也可以设置为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","fk-1:9092");
        prop.setProperty("group.id","flink_consumer");
        prop.setProperty("auto.offset.reset","latest");

        //消费多个topic
        List<String> topics = new ArrayList<>();
        topics.add("test");

        DataStreamSource<ConsumerRecord<String, String>> udfStream = env.addSource(new FlinkKafkaConsumer<>(topics, new MessageDeSerializationSchema(), prop));
        udfStream.map(new MapFunction<ConsumerRecord<String, String>, Object>() {
            @Override
            public Object map(ConsumerRecord<String, String> record) throws Exception {
                return new Tuple2<String, String>(record.key(),record.value());
            }
        }).print();

        env.execute("udf-consumer");
    }
}
