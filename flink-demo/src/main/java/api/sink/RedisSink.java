package api.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author zdp
 * @description save data to redis
 * @email 13221018869@189.cn
 * @date 2021/5/27 19:26
 */
public class RedisSink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        //kafka prop
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "fk-1:9092");
        prop.setProperty("group.id", "flink_consumer");
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("enable.auto.commit", "true");

        //kafka topics
        ArrayList<String> topics = new ArrayList<>();
        topics.add("words");

        //flink link to kafka
        DataStreamSource<String> lines = env.addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop));
        //datastream operation

        //jedis prop
        FlinkJedisPoolConfig jediConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("fk-1")
                .setPort(6379)
                .setDatabase(0)
                .build();
        // save data to redis
        lines.addSink(new org.apache.flink.streaming.connectors.redis.RedisSink(jediConfig,new myRedisMapper()));
        env.execute();

    }

    static class myRedisMapper implements RedisMapper<Tuple2<String,Integer>> {
        /**
         * ???????????????redis????????????????????????key?????????
         * ??????RedisCommand????????????????????????
         * Returns descriptor which defines data type.
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            //??????????????????redis??????????????????HASH?????????????????????????????????HASH????????????key
            return new RedisCommandDescription(RedisCommand.HSET,"flink_test");
        }

        /**
         * ??????value??????????????? key??????
         * Extracts key from data.
         */
        @Override
        public String getKeyFromData(Tuple2<String,Integer> t) {
            //HASH???????????????key
            return t.f0;
        }

        /**
         * ??????value??????????????? value??????
         * Extracts value from data.
         */
        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            return t.f1+"";
        }
    }
}

