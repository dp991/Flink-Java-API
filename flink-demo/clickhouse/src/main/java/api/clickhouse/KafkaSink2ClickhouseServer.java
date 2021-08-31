package api.clickhouse;

import api.clickhouse.domain.TrunkFlowDto;
import api.clickhouse.utils.Helper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zdp
 * @description 读取kafka数据保存到clickhouse
 * @email 13221018869@189.cn
 * @date 2021/6/30 9:40
 */
public class KafkaSink2ClickhouseServer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //指定checkpoint的触发间隔,必须设置checkpoint保存路径
        env.enableCheckpointing(5000);
        // 默认的CheckpointingMode就是EXACTLY_ONCE，也可以指定为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //"hdfs://fk-1:9000/backhand"
        //env.setStateBackend(new FsStateBackend("hdfs://10.50.51.3:8020/flink/backhand"));
        env.setStateBackend(new FsStateBackend("file:///usr/local/apps/checkpoints"));

        //任务重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(6, Time.seconds(5))
        );

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.50.51.72:9092,10.50.51.79:9092,10.50.51.80:9092");

        //设置消费者组
        properties.setProperty("group.id", "flink-group323");

        //消费的三种方式，默认是latest
        //earliest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //latest：各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //none： topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常

        properties.setProperty("auto.offset.reset", "earliest");

//        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
//        DataStreamSource stream = env.addSource(kafkaConsumer).setParallelism(2);


        //消费多个topic
        List<String> topics = new ArrayList<>();

        topics.add("cloud_switch_msg_type_5");

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> flowDtoSingleOutputStreamOperator = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                List<TrunkFlowDto> list = Helper.string2Dto(s);
                if (list.size() != 0) {
                    list.stream().forEach(dto -> {
                        collector.collect(TrunkFlowDto.convertToCsv(dto));
//                        System.out.println(count.incrementAndGet());
                    });
                }
            }
        });

        Map<String, String> globalParameters = new HashMap<>();

        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://10.50.51.46:8123/");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "default");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "123456");

        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "/usr/local/apps/flink2ch-2");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "2");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);

        // create props for sink
        Properties props = new Properties();

        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "cloud_switch_msg.cloud_switch_msg_type_5");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "1000");

        ClickHouseSink sink = new ClickHouseSink(props);

        flowDtoSingleOutputStreamOperator.addSink(sink).name("ck-sink").setParallelism(3);

//        flowDtoSingleOutputStreamOperator.print();

        env.execute("kafka-flink-clickhouse");
    }
}
