package api.cep;

import api.cep.domain.Trunk;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import stream.AdClickEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author zdp
 * @description liuliang
 * @email 13221018869@189.cn
 * @date 2021/6/28 11:34
 */
public class TrunkWarnState {

    final static OutputTag<Trunk> warnOutputTag = new OutputTag<Trunk>("trunk-warn") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\work\\projects\\flink-demo\\src\\main\\resources\\test.txt";


        DataStreamSource<String> socketTextStream = env.readTextFile(path);

        SingleOutputStreamOperator<Trunk> streamOperator = socketTextStream.filter(new FilterFunction<String>() {
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

        SingleOutputStreamOperator<Trunk> valueStateDescriptor = streamOperator.keyBy(Trunk::getUser).process(new ProcessFunction<Trunk, Trunk>() {

            private long throld = 600;
            ValueState state = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //???open??????????????????valueState??????
                RuntimeContext runtimeContext = getRuntimeContext();
                ValueStateDescriptor<Tuple2<Long, Trunk>> descriptor = new ValueStateDescriptor<>("valueStateDescriptor", TypeInformation.of(new TypeHint<Tuple2<Long, Trunk>>() {
                }));
                state = runtimeContext.getState(descriptor);
            }

            @Override
            public void processElement(Trunk trunk, Context context, Collector<Trunk> collector) throws Exception {
                //?????????valueState??????????????????
                Tuple2<Long, Trunk> historyData = (Tuple2<Long, Trunk>) state.value();
                //???????????????????????????value?????????valueState??????????????????????????????????????????????????????valueState???
                if (historyData == null) {
                    state.update(new Tuple2<Long, Trunk>(trunk.getTime(), trunk));
                } else {
                    //?????????
                    long historyTrunkValue = historyData.f1.getTrunkValue();
                    if (Math.abs(historyTrunkValue - trunk.getTrunkValue()) > throld) {
                        //????????????
                        context.output(warnOutputTag, trunk);
                    }
                    state.update(new Tuple2<Long, Trunk>(trunk.getTime(), trunk));
                }

            }
        });

        valueStateDescriptor.getSideOutput(warnOutputTag).print("warn");

        env.execute();


    }
}
