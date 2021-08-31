package test;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class MainServer {

    public static Logger logger = LoggerFactory.getLogger(MainServer.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> stream = env.readTextFile("D:\\work\\projects\\flink-demo\\clickhouse\\src\\main\\resources\\t.txt");

        SingleOutputStreamOperator<T> TStream = stream.map(new MapFunction<String, T>() {
            @Override
            public T map(String s) throws Exception {
                String[] s1 = s.split(",");
                return new T(Integer.parseInt(s1[0]), s1[1], s1[2]);
            }
        });

        JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(10);

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connection = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        connection.withUrl("jdbc:clickhouse://10.50.51.46:8123")
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .withUsername("default")
                .withPassword("123456");

        String sql = "insert into cloud_switch_msg.t values(?,?,?)";
        //sink
        TStream.addSink(JdbcSink.sink(sql, new TSinkCKBuilder(), builder.build(), connection.build())).name("clickhouse");

        logger.error("test started");
        logger.info("starttttttttt");

        env.execute("test");

    }
}
