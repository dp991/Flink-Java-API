package api.state;

import api.state.entity.AggregatingStateMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author zdp
 * @description 统计部门的平均工资
 * @email 13221018869@189.cn
 * @date 2021/6/3 15:26
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

//        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .build();
//        ValueStateDescriptor<String> textState = new ValueStateDescriptor<>("text state", String.class);
//        textState.enableTimeToLive(ttlConfig);
//
//        StateTtlConfig.newBuilder(Time.seconds(1)).cleanupFullSnapshot().build();
//
//        StateTtlConfig.newBuilder(Time.seconds(1)).cleanupIncrementally(10,true).build();
//
//        StateTtlConfig.newBuilder(Time.seconds(1)).cleanupInRocksdbCompactFilter(1000).build();

        DataStreamSource<String> stream = env.socketTextStream("fk-1", 9099);

        //传入数据：部门 姓名 工资
        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Double>> out) throws Exception {
                String[] info = s.split(" ");
                out.collect(new Tuple2<>(info[0], Double.parseDouble(info[2])));
            }
        }).keyBy(dp -> dp.f0).map(new AggregatingStateMapFunction()).print();

        env.execute();
    }
}
