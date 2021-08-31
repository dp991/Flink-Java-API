package api.cep;

import api.cep.domain.Trunk;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;


/**
 * @author zdp
 * @description trunk-warn winds implement
 * @email 13221018869@189.cn
 * @date 2021/6/28 14:04
 */

/**
 * 流量突变预警
 */
public class TrunkWarnWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\work\\projects\\flink-demo\\src\\main\\resources\\test.txt";


        DataStreamSource<String> socketTextStream = env.readTextFile(path);

        DataStream<Trunk> trunkStringKeyedStream = socketTextStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                String[] s1 = s.split(" ");
                return s1.length == 3 ? true : false;
            }
        }).map(new MapFunction<String, Trunk>() {
            @Override
            public Trunk map(String s) throws Exception {
                String[] strings = s.split(" ");
                return new Trunk(strings[0], Long.parseLong(strings[1]), Long.parseLong(strings[2]));
            }
        });

        //设置watermark和指定事件时间字段，最大延迟1s
        DataStream<Trunk> watermakerDS = trunkStringKeyedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Trunk>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTime() * 1000)
                );

        //分组
        KeyedStream<Trunk, String> keyedStream = watermakerDS.keyBy(new KeySelector<Trunk, String>() {
            @Override
            public String getKey(Trunk trunk) throws Exception {
                return trunk.getUser();
            }
        });

        //滚动窗口，窗口大小为30s
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(30))).process(new ProcessWindowFunction<Trunk, Trunk, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Trunk> iterable, Collector<Trunk> collector) throws Exception {
                List<Trunk> list = new ArrayList<>();

                Iterator<Trunk> iterator = iterable.iterator();
                while (iterator.hasNext()) {
                    list.add(iterator.next());
                }
                //排序
                Collections.sort(list, new Comparator<Trunk>() {
                    @Override
                    public int compare(Trunk o1, Trunk o2) {
                        return o1.getTime() > o2.getTime() ? 1 : -1;
                    }
                });
                //遍历
//                collector.collect(list);
                for (int i = 0; i < list.size() - 1; i++) {
                    if (list.size() == 1) {
                        break;
                    }
                    Trunk current = list.get(i);
                    Trunk next = list.get(i + 1);
                    if (Math.abs(current.getTrunkValue() - next.getTrunkValue()) > 500) {
                        System.out.println("compare:" + current + "," + list.get(i + 1) + ",size:" + list.size());
                        collector.collect(list.get(i + 1));
                    }
                    if (list.size() == 2) {
                        break;
                    }

                }
            }
        }).print("告警");

        env.execute();

    }

}
