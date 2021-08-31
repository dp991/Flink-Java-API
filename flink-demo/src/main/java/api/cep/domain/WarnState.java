package api.cep.domain;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @author zdp
 * @description warn-state
 * @email 13221018869@189.cn
 * @date 2021/6/29 13:42
 */
public class WarnState extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    ValueState state = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //在open方法里，创建valueState对象
        RuntimeContext runtimeContext = getRuntimeContext();
        ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("valueStateDescriptor", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));
        state = runtimeContext.getState(descriptor);
    }

    /**
     * 使用state 完成wordcount
     *
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
        //获取到valueState中存储的数据
        Tuple2<String, Integer> historyData = (Tuple2<String, Integer>) state.value();
        //把当前输入的数据（value），和valueState中的数据累加，把累加之后的数据放入到valueState中
        if (historyData == null) {
            state.update(value);
        } else {
            state.update(new Tuple2<String, Integer>(value.f0, value.f1 + historyData.f1));
        }
        return (Tuple2<String, Integer>) state.value();
    }
}