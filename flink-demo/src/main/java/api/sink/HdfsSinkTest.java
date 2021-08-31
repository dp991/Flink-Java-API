package api.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author zdp
 * @description hdfs sink test
 * @email 13221018869@189.cn
 * @date 2021/5/27 15:09
 */
public class HdfsSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "fk-1:9092");

        //设置消费者组
        properties.setProperty("group.id", "flink-group");

        //消费的三种方式，默认是latest
        //earliest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none： topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常

        properties.setProperty("auto.offset.reset", "latest");

//        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
//        DataStreamSource stream = env.addSource(kafkaConsumer).setParallelism(2);


        //消费多个topic
        List<String> topics = new ArrayList<>();
        topics.add("test");
        topics.add("words");
        DataStream<String> streamBatch = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties));

//        String localPath = "D:\\work\\projects\\flink-demo\\src\\main\\resources\\word.txt";
//        DataStreamSource<String> inputStream = env.readTextFile(localPath);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamBatch.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                Arrays.stream(words).forEach(w -> {
                    collector.collect(new Tuple2<>(w, 1));
                });

            }
        }).keyBy(t -> t.f0).sum(1);

        sum.print();

        /**
         * 保存到socket
         */
//        DataStreamSink<Tuple2<String, Integer>> socketSink = sum.writeToSocket("localhost", 9099, new SerializationSchema<Tuple2<String, Integer>>() {
//            @Override
//            public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
//                return new byte[0];
//            }
//        });

        /**
         * 保存到hdfs文件系统
         */
        String hdfsPath = "hdfs://fk-1:9000/in";
        StreamingFileSink<Tuple2<String, Integer>> hdfsSink = StreamingFileSink.forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()).withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd")).build();

        sum.addSink(hdfsSink).setParallelism(1);
        env.execute();

    }
}
