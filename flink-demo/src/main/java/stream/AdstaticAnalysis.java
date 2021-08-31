package stream;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import stream.utils.DateUtil;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: create by zdp
 * @version: v1.0
 * @description: PACKAGE_NAME
 * @date:2020-02-25 广告分析，广告点击量统计
 */
public class AdstaticAnalysis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置时间语意,flink-1.12.0开始默认支持EventTime，所以不用在设置

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //source
        String path = "D:\\work\\projects\\flink-demo\\src\\main\\resources\\adclick.txt";
        DataStreamSource<String> streamSource = env.readTextFile(path);

        SingleOutputStreamOperator<AdClickEvent> adEventStream = streamSource.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String[] strings = s.split(",");
                long userId = Long.valueOf(strings[0]);
                long adId = Long.valueOf(strings[1]);
                String pro = strings[2];
                String city = strings[3];
                long timestamp = Long.valueOf(strings[4]);
                return new AdClickEvent(userId, adId, pro, city, timestamp);
            }
        });

        //指定时间戳字段,并生成watermarks，允许最大的滞后时间为1s，超过该时间的数据将被忽略
        SerializableTimestampAssigner<AdClickEvent> timestampAssigner = new SerializableTimestampAssigner<AdClickEvent>() {
            @Override
            public long extractTimestamp(AdClickEvent adClickEvent, long l) {
                return adClickEvent.getTimestamp()*1000;
            }
        };

        SingleOutputStreamOperator<AdClickEvent> adClickEventSingleOutputStream =
                adEventStream.assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(timestampAssigner));

        //根据省份做分组，进行开窗聚合
        KeyedStream<AdClickEvent, String> adCountStream = adClickEventSingleOutputStream.keyBy(new KeySelector<AdClickEvent, String>() {
            @Override
            public String getKey(AdClickEvent adClickEvent) throws Exception {
                return adClickEvent.getProvince();
            }
        });

//        //滑动窗口，窗口大小为2s，步长为1s
        adCountStream.window(SlidingEventTimeWindows.of(Time.seconds(2),Time.seconds(1)));
//        //滚动窗口，窗口大小为2s
//        adCountStream.window(TumblingEventTimeWindows.of(Time.seconds(2)));
//        //countWindow,4个元素为一个窗口
//        adCountStream.countWindow(4);
//        //countWindow,4个元素为一个窗口,步长为2
//        adCountStream.countWindow(4,2);
//        //绘画窗口
//        adCountStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(100)));

        //开窗,滑动窗口，统计一个小时内的数据，每5s滑动一次,timeWindow已过时
//        WindowedStream<AdClickEvent, String, TimeWindow> adClickEventStringTimeWindowWindowedStream = adCountStream.timeWindow(Time.hours(1), Time.seconds(5));

        WindowedStream<AdClickEvent, String, TimeWindow> adClickEventStringTimeWindowWindowedStream = adCountStream.window(TumblingEventTimeWindows.of(Time.seconds(2)));

        //聚合AggregateFunction:AdClickEvent输入类型，第一个long累加器类型，第二个long输出类型
        SingleOutputStreamOperator<CountByProvince> aggregate = adClickEventStringTimeWindowWindowedStream.aggregate(new AggregateFunction<AdClickEvent, Long, Long>() {
            /**
             * 累加器初始化
             *
             * @return
             */
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            /**
             * 累计器加1
             *
             * @param adClickEvent
             * @param l
             * @return
             */
            @Override
            public Long add(AdClickEvent adClickEvent, Long l) {
                return l + 1;
            }

            @Override
            public Long getResult(Long l) {
                return l;
            }

            /**
             * 累加器合并
             *
             * @param l
             * @param acc1
             * @return
             */
            @Override
            public Long merge(Long l, Long acc1) {
                return l + acc1;
            }
        }, new WindowFunction<Long, CountByProvince, String, TimeWindow>() {
            //WindowFunction参数类型：Long累加器输出类型，CountByProvince：WindowFunction输出类型，String：keyBy字段类型
            @Override
            public void apply(String s, TimeWindow window, Iterable<Long> iterable, Collector<CountByProvince> out) throws Exception {
                out.collect(new CountByProvince(DateUtil.long2String(window.getStart()),DateUtil.long2String(window.getEnd()), s, iterable.iterator().next()));
            }
        });

        aggregate.print();
        env.execute("ad analysis job");
    }
}
