package api.state.broadcast;

import api.state.broadcast.domain.Employee;
import api.state.broadcast.domain.Rule;
import api.state.broadcast.domain.SaleValue;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zdp
 * @description KeyedStreamBroadcast
 * @email 13221018869@189.cn
 * @date 2021/6/24 19:59
 */

/**
 * 4个泛型分别是
 * <KS> The key type of the input keyed stream. KeyedStream 中 key 的类型
 * <IN1> The input type of the keyed (non-broadcast) side. 即非广播流的类型
 * <IN2> The input type of the broadcast side. 广播流的类型
 * <OUT> The output type of the operator. 输出流的元素类型
 */
public class KeyedStreamBroadcast extends KeyedBroadcastProcessFunction<String, SaleValue, Rule, Employee> {

    MapStateDescriptor<String, String> mapStateDescriptor = null;
    OutputTag<String> outputTag = null;

    //定义ReducingState保存员工销售额
    ReducingState saleTotalAmountStat = null;

    public KeyedStreamBroadcast(OutputTag<String> outputTag, MapStateDescriptor<String, String> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        saleTotalAmountStat = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Double>("userTotalAmount", new ReduceFunction<Double>() {
            @Override
            public Double reduce(Double d1, Double d2) throws Exception {
                return d1 + d2;
            }
        }, TypeInformation.of(Double.class)));
    }

    //处理金额流
    @Override
    public void processElement(SaleValue saleValue, ReadOnlyContext readOnlyContext, Collector<Employee> out) throws Exception {
        ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //将本次销售金额累计到历史记录
        saleTotalAmountStat.add(saleValue.getSale());
        if (broadcastState != null && broadcastState.contains(saleValue.getDepartmentName())) {
            //类别下对应的阈值
            Double threshold = Double.parseDouble(broadcastState.get(saleValue.getDepartmentName()));
            Double saleTotalAmount = Double.parseDouble(saleTotalAmountStat.get().toString());
            if (saleTotalAmount > threshold) {
                //符合奖励规则，将符合奖励规则的用户输出到下游
                out.collect(new Employee(saleValue.getEmployeeId(), saleValue.getEmployeeName(), saleValue.getDepartmentName()));
            } else {
                //不符合奖励规则,outupttag
                readOnlyContext.output(outputTag, saleValue.getEmployeeId() + "_" + saleValue.getEmployeeName() + "----还差" + (threshold - saleTotalAmount) + "就可以获得奖励");
            }

        } else {
            //部门下还没有设置奖励规则
            readOnlyContext.output(outputTag,"奖励规则制定中...");
        }
    }

    //处理广播流
    @Override
    public void processBroadcastElement(Rule rule, Context context, Collector<Employee> collector) throws Exception {
        BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(rule.getDepartmentName(),rule.getThreshold().toString());
    }
}
