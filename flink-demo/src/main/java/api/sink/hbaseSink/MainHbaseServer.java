package api.sink.hbaseSink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MainHbaseServer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9099);

        SingleOutputStreamOperator<Tuple2<String, String>> streamOperator = socketTextStream.map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[0], split[1]);
            }
        }).setParallelism(1);


        RichSinkFunction sink = new MyHbaseSink(2);

        streamOperator.addSink(sink).name("hbase sink").setParallelism(1);

        env.execute();
    }
}
