package api.source.java;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zdp
 * @description 从hadoop读取文件
 * @email 13221018869@189.cn
 * @date 2021/5/26 11:04
 */
@Slf4j
public class HadoopFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        log.info("started");
        DataStreamSource<String> streamFromDFS = env.readTextFile("hdfs://fk-1:9000/in/adclick.txt");

        streamFromDFS.print();

        log.info("completed");
        env.execute("read content from hadoop file");
    }
}
