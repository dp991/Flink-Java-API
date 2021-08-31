package api.failurRestart;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @author zdp
 * @description failur restart
 * @email 13221018869@189.cn
 * @date 2021/6/3 14:09
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //打开checkpoint开关，默认是无限次重启的
        env.enableCheckpointing(500);

        //第一种：固定延迟重启
        //设置最多尝试重启6次，第7次异常发生时，程序退出
        //任务失败5秒后开始执行重启操作
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(6, Time.seconds(5))
        );

        //第二种：失败率重启
        //5分钟内，最多尝试重启10次，第10次异常发生时，程序退出
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(
//                        10,//最多尝试重启10次，第10次异常发生时，程序退出
//                        Time.minutes(5),//计算失败次数的时间范围：5分钟
//                        Time.seconds(10))//失败10秒后再尝试重启
//        );

        //第三种：不重启
//        env.setRestartStrategy(
//                RestartStrategies.noRestart()
//        );

        DataStreamSource<String> lines = env.socketTextStream("fk-1", 9001);
        lines.flatMap((String words, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(words.split(" ")).forEach(word -> {
                if (word.equals("aaa")) {
                    System.out.println("发生了异常");
                    throw new RuntimeException("手动异常");
                }
                out.collect(Tuple2.of(word, 1));
            });
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(x -> x.f0).sum(1).print();

        env.execute();

    }
}
