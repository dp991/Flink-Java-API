package api.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author zdp
 * @description kafak sink
 * @email 13221018869@189.cn
 * @date 2021/5/27 19:51
 */

public class KafkaSinkTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

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
        lines.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "fk-1:9092");
        properties.setProperty("zookeeper.connect", "fk-1:2181");
        properties.setProperty("group.id", "flink_consumer");

        //sink to test topic
        lines.addSink(new FlinkKafkaProducer<String>("fk-1:9092","test",new SimpleStringSchema())).name("flink-kafka").setParallelism(1);

        env.execute();
    }
}
