package api.state.entity;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @author zdp
 * @description 统计部门的平均工资
 * @email 13221018869@189.cn
 * @date 2021/6/3 15:27
 */
public class AggregatingStateMapFunction extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
    //输入Tuple2<String, Double>，输出Tuple2<String, Double>
    AggregatingState<Double, Double> aggregatingState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建AggregatingStateDescriptor对象
        /**
         * in = Double 输入类型
         * ACC = Tuple2<Integer, Double> 累加器类型
         * out = Double 输出类型
         */
        AggregatingStateDescriptor<Double, Tuple2<Integer, Double>, Double> descriptor = new AggregatingStateDescriptor<>("agg", new AggregateFunction<Double, Tuple2<Integer, Double>, Double>() {
            @Override
            public Tuple2<Integer, Double> createAccumulator() {
                return new Tuple2<>(0, 0.0);
            }

            @Override
            public Tuple2<Integer, Double> add(Double value, Tuple2<Integer, Double> acc) {
                return new Tuple2<>(acc.f0 + 1, acc.f1 + value);
            }
            //求平均值
            @Override
            public Double getResult(Tuple2<Integer, Double> tuple2) {
                return tuple2.f1 / tuple2.f0;
            }
            //聚合
            @Override
            public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) {
                return new Tuple2<>(t1.f0 + t1.f0, t1.f1 + t2.f1);
            }
        }, TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
        }));
        //获取RuntimeContext对象
        RuntimeContext runtimeContext = getRuntimeContext();
        //获取AggregatingState对象
        aggregatingState = runtimeContext.getAggregatingState(descriptor);

    }

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> tuple2) throws Exception {
        aggregatingState.add(tuple2.f1);
        return new Tuple2<String, Double>(tuple2.f0, aggregatingState.get());
    }
}
