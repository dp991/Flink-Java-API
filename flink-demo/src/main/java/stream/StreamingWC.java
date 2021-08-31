package stream;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamingWC {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env=StreamExecutionEnvironment.createRemoteEnvironment("192.168.1.114",8081,"D:\\work\\projects\\flink-demo\\target\\flink-demo-1.0-SNAPSHOT-jar-with-dependencies.jar");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        ExecutionConfig config = env.getConfig();
        //ProcessingTime
        config.setAutoWatermarkInterval(2000);

//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStreamSource<String> textStream = env.socketTextStream("192.168.191.3", 9099);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tupeStream = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        tupeStream.keyBy(x-> x.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1+t2.f1);
            }
        }).print();

        tupeStream.keyBy(x -> x.f0).max(1).print();
        tupeStream.keyBy(x -> x.f0).maxBy(1).print();

        env.execute("wc");
    }
}
