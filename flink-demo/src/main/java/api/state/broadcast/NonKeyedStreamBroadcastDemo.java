package api.state.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author zdp
 * @description NonKeyedStreamBroadcastDemo
 * @email 13221018869@189.cn
 * @date 2021/6/24 19:27
 */
public class NonKeyedStreamBroadcastDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //dataSource
        //高吞吐量流：non-broadcasted stream
        DataStreamSource<String> highThroughputStream = env.socketTextStream("fk-1", 1000);

        //低吞吐量流：broadcastStream，需要通过broadcast方法获取
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> lowThroughputStream = env.socketTextStream("fk-1", 9999).broadcast(mapStateDescriptor);

        //non-broadcasted stream通过connect方法连接broadcastStream，得到BroadcastConnectedStream
        BroadcastConnectedStream<String, String> broadcastConnectedStream = highThroughputStream.connect(lowThroughputStream);
        OutputTag<String> nonMatchTag = new OutputTag<String>("non-match"){};

        SingleOutputStreamOperator<String> dataStream = broadcastConnectedStream.process(new NonKeyedStreamBroadcast(nonMatchTag, mapStateDescriptor));

        dataStream.print("匹配规则");
        dataStream.getSideOutput(nonMatchTag).print("不匹配规则");
        env.execute();


    }
}
