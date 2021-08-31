package api.source.java;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @author zdp
 * @description 读取本地文件数据
 * @email 13221018869@189.cn
 * @date 2021/5/26 10:02
 */
@Slf4j
public class LocalFileTest {

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(50000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://fk-1:9000/backhand"));
        log.info("peizhisdlsjd");

        DataStreamSource<String> stream1 = env.readTextFile("D:\\work\\projects\\flink-demo\\src\\main\\resources\\adclick.txt");
        DataStreamSource<String> stream2 = env.readTextFile("D:\\work\\projects\\flink-demo\\src\\main\\resources\\test.txt");

        DataStream<Tuple2<String, String>> t1 = stream1.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new Tuple2<>(split[0], split[1]));
            }
        });
        SingleOutputStreamOperator<Tuple2<String, String>> t2 = stream2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], split[1]);
            }
        });
        //t1:(string,string),t2:(string,string)

        //定义一个输出标签
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        OutputTag<String> correctTag = new OutputTag<String>("correct"){};
        OutputTag<String> all = new OutputTag<String>("all"){};
        SingleOutputStreamOperator<String> splitStream = t1.process(new ProcessFunction<Tuple2<String, String>, String>() {
            @Override
            public void processElement(Tuple2<String, String> t, Context context, Collector<String> collector) throws Exception {
                if (t.f1.contains("1766")) {
                    context.output(errorTag, t.f0);
                }
                if (t.f1.contains("1756")) {
                    context.output(correctTag, t.f0);
                }
                //消息发给下游
                collector.collect(t.f1);
            }
        });
        splitStream.getSideOutput(errorTag).printToErr("错误信息");
        splitStream.getSideOutput(correctTag).printToErr("正确信息");
        splitStream.print("所有信息");
        env.execute("read local file");
    }
}
