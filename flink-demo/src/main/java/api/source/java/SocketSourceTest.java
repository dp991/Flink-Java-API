package api.source.java;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zdp
 * @description 读取socket消息
 * @email 13221018869@189.cn
 * @date 2021/5/26 17:02
 */
public class SocketSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9099);

        textStream.print();

        env.execute();
    }
}
