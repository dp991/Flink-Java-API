package api.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author zdp
 * @description receive data from kafka and save it to hdfs(kafka source,hdfs sink)
 * @email 13221018869@189.cn
 * @date 2021/5/27 18:17
 */
public class KafkaSourceHdfsSinkTest {
    public static void main(String[] args) throws Exception{

        //flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(50000,CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://fk-1:9000/backhand"));

        //kafka prop
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "fk-1:9092");
        prop.setProperty("group.id", "flink_consumer");
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("enable.auto.commit", "true");

        //kafka topics
        ArrayList<String> topics = new ArrayList<>();
        topics.add("words");

        //flink link to kafka
        DataStreamSource<String> lines = env.addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop));


        //datastream operation: wordcount
        SingleOutputStreamOperator<Tuple2<String, Integer>> rs = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                Arrays.stream(words).forEach(word -> {
                    collector.collect(new Tuple2<>(word, 1));
                });
            }
        }).keyBy(t -> t.f0).sum(1);

        rs.print();

        //save to hdfs
        String hdfsPath="hdfs://fk-1:9000/yyds";
        StreamingFileSink<Tuple2<String, Integer>> hdfsSink = StreamingFileSink.forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()).withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd")).build();

        rs.addSink(hdfsSink).setParallelism(1);
        env.execute();
    }
}
