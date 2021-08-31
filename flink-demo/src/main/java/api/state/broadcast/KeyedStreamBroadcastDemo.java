package api.state.broadcast;

import api.state.broadcast.domain.Employee;
import api.state.broadcast.domain.Rule;
import api.state.broadcast.domain.SaleValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zdp
 * @description KeyedStreamBroadcastDemo
 * @email 13221018869@189.cn
 * @date 2021/6/25 9:40
 */
public class KeyedStreamBroadcastDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //高吞吐量流
        //数据输入要求： 按照销售额类中的顺序输入
        //员工id 员工姓名 所属部门 销售额
        DataStreamSource<String> highThroughputStream = env.socketTextStream("fk-1", 9999);

        KeyedStream<SaleValue, String> nonBroadcastStream = highThroughputStream.flatMap(new FlatMapFunction<String, SaleValue>() {
            @Override
            public void flatMap(String s, Collector<SaleValue> collector) throws Exception {
                String[] strings = s.split(" ");
                collector.collect(new SaleValue(strings[0], strings[1], strings[2], Double.parseDouble(strings[3])));
            }
        }).keyBy(slave -> slave.getEmployeeName());

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastStreamMapStateDescriptor", String.class, String.class);
        //低吞吐量流
        //数据输入要求：按照Rule类的属性顺序输入
        //erp 5000   os 4000   嵌入式 3000
        DataStreamSource<String> lowThroughputStream = env.socketTextStream("fk-1", 9998);

        BroadcastStream<Rule> broadcastStream = lowThroughputStream.flatMap(new FlatMapFunction<String, Rule>() {
            @Override
            public void flatMap(String s, Collector<Rule> collector) throws Exception {
                String[] s1 = s.split(" ");
                collector.collect(new Rule(s1[0], Double.parseDouble(s1[1])));
            }
        }).broadcast(mapStateDescriptor);

        //连接
        BroadcastConnectedStream<SaleValue, Rule> broadcastConnectedStream = nonBroadcastStream.connect(broadcastStream);

        OutputTag<String> outputTag = new OutputTag<String>("没有奖励") {
        };
        //process

        SingleOutputStreamOperator<Employee> operator = broadcastConnectedStream.process(new KeyedStreamBroadcast(outputTag, mapStateDescriptor));

        operator.print("奖励员工：");
        operator.getSideOutput(outputTag).print("没有奖励");

        env.execute();

    }
}
