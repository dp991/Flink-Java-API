package api.state.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zdp
 * @description NonKeyedStreamBroadcast
 * @email 13221018869@189.cn
 * @date 2021/6/24 19:01
 */
public class NonKeyedStreamBroadcast extends BroadcastProcessFunction<String, String, String> {

    MapStateDescriptor<String, String> confDescriptor = null;
    OutputTag<String> outputTag = null;

    public NonKeyedStreamBroadcast(OutputTag<String> outputTag, MapStateDescriptor<String, String> confDescriptor) {
        this.confDescriptor = confDescriptor;
        this.outputTag = outputTag;
    }

    /**
     * 处理低吞吐量流
     *
     * @param s       低吞吐量流对应的数据
     * @param context
     * @param out
     */
    @Override
    public void processBroadcastElement(String s, Context context, Collector<String> out) throws Exception {
        //把broadcastStream中的数据(过滤规则)放入到broadcastState中 广播出去
        BroadcastState<String, String> broadcastState = context.getBroadcastState(confDescriptor);
        broadcastState.put("rule", s);
    }

    /**
     * 处理高吞吐量流
     *
     * @param s               高吞吐量流对应的数据
     * @param readOnlyContext
     * @param out
     */
    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> out) throws Exception {

        //获取到只读broadcastState对象
        ReadOnlyBroadcastState<String, String> readOnlyContextBroadcastState = readOnlyContext.getBroadcastState(confDescriptor);
        if (readOnlyContextBroadcastState.contains("rule")) {
            if (s.contains(readOnlyContextBroadcastState.get("rule"))) {
                //non-broadcastStream中符合过滤规则
                out.collect("过滤规则是：" + readOnlyContextBroadcastState.get("rule") + ",符合过滤规则的数据是：" + s);
            } else {
                //
                readOnlyContext.output(outputTag, s);
            }
        } else {
            System.out.println(("rule 判断规则不存在"));
            //通过side out将数据输出
            //通过side out将数据输出
            readOnlyContext.output(outputTag, s);
        }

    }
}
