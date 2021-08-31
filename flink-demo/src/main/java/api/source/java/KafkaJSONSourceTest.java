package api.source.java;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import scala.util.parsing.json.JSON;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author zdp
 * @description kafka topic 数据必须是json格式数据
 * @email 13221018869@189.cn
 * @date 2021/5/26 19:39
 */
public class KafkaJSONSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "fk-1:9092");
        prop.setProperty("group.id", "flink_consumer");
        prop.setProperty("auto.offset.reset", "latest");

        ArrayList<String> topics = new ArrayList<>();
        topics.add("test");
        //参数为true表示获取元数
//        DataStreamSource<String> jsonStream = env.addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop));

        DataStreamSource<ObjectNode> jsonStream = env.addSource(new FlinkKafkaConsumer<>(topics, new JSONKeyValueDeserializationSchema(false), prop));
        jsonStream.print();
        env.execute();
    }
}
