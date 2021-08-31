package api.state.entity;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @author zdp
 * @description ReducingStateMapFunction
 * @email 13221018869@189.cn
 * @date 2021/6/2 20:30
 */
public class ReducingStateMapFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    /**
     * ReducingState[T]和AggregatingState[IN, OUT]与ListState[T]同属于MergingState[T]。
     * 与ListState[T]不同的是，ReducingState[T]只有一个元素，而不是一个列表。它的原理是新元素通过add(value: T)加入后，
     * 与已有的状态元素使用ReduceFunction合并为一个元素，并更新到状态里。
     * AggregatingState[IN, OUT]与ReducingState[T]类似，也只有一个元素，只不过AggregatingState[IN, OUT]的输入和输出类型可以不一样。
     * ReducingState[T]和AggregatingState[IN, OUT]与窗口上进行ReduceFunction和AggregateFunction很像，都是将新元素与已有元素做聚合。
     */

    ReducingState reducingState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建ReducingStateDescrptor
        ReducingStateDescriptor<Integer> rsd = new ReducingStateDescriptor<>("RSD", new ReduceFunction<Integer>() {
            //重写reduce方法
            @Override
            public Integer reduce(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }, TypeInformation.of(Integer.class));
        //获取RuntimeContext
        RuntimeContext context = getRuntimeContext();
        //获取ReducingState对象
        reducingState = context.getReducingState(rsd);
    }

    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
        //存入状态,并合并成一个元素，怎么合并，看上面重写的reduce方法
        reducingState.add(tuple2.f1);
        return new org.apache.flink.api.java.tuple.Tuple2<String,Integer>(tuple2.f0,(Integer) reducingState.get());
    }
}
