package api.sink;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author zdp
 * @description 保存到本地文件系统
 * @email 13221018869@189.cn
 * @date 2021/5/27 9:05
 */
public class LocalFileSinkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> inputStream = env.readTextFile("D:\\work\\projects\\flink-demo\\src\\main\\resources\\adclick.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = inputStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                collector.collect(new Tuple2<>(words[2], 1));
            }
        }).keyBy(t -> t.f0).sum(1);

//        sum.print();


        /**
         * Note that the write*() methods on DataStream are mainly intended for debugging purposes. They are not participating
         * in Flink’s checkpointing, this means these functions usually have at-least-once semantics. The data flushing to
         * the target system depends on the implementation of the OutputFormat. This means that not all elements send to
         * the OutputFormat are immediately showing up in the target system. Also, in failure cases, those records might be lost.
         *
         * For reliable, exactly-once delivery of a stream into a file system, use the StreamingFileSink.
         * Also, custom implementations through the .addSink(...) method can participate in Flink’s checkpointing for
         * exactly-once semantics.
         */

        /**
         * Row-encoded Formats
         */
        String fileSinkPath = "D:\\work\\projects\\flink-demo\\src\\main\\resources";
        StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink.forRowFormat(new Path(fileSinkPath), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()).withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH-mm")).build();
        sum.addSink(sink).setParallelism(1);

        /**
         * Bulk-encoded Formats
         * 反射的方式太慢：ParquetAvroWriters.forReflectRecord(Word.class)
         *
         * BasePathBucketAssigner ，不分桶，所有文件写到根目录；
         *
         * DateTimeBucketAssigner ，基于系统时间分桶。
         *
         * Flink 提供了两个滚动策略，滚动策略实现了 org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy 接口：
         *
         * DefaultRollingPolicy 当超过最大桶大小（默认为 128 MB），或超过了滚动周期（默认为 60 秒），或未写入数据处于不活跃状态超时（默认为 60 秒）的时候，滚动文件；
         *
         * OnCheckpointRollingPolicy 当 checkpoint 的时候，滚动文件
         *
         */
//        SingleOutputStreamOperator<Word> wordCount = sum.map(t -> new Word(t.f0, t.f1));
//
//        DateTimeBucketAssigner<Word> bucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd");
//
//        StreamingFileSink<Word> bulkSink = StreamingFileSink.forBulkFormat(new Path(fileSinkPath), ParquetAvroWriters.forReflectRecord(Word.class)).withBucketAssigner(bucketAssigner).build();
//        wordCount.addSink(bulkSink).setParallelism(1);

        /**
         * Avro format / ORC Format / Hadoop SequenceFile format
         */
        env.execute();
    }
}
