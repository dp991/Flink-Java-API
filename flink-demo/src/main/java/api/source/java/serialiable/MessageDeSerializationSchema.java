package api.source.java.serialiable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author zdp
 * @description 自定义消费序列
 * String, String是key/value的类型
 * @email 13221018869@189.cn
 * @date 2021/5/26 14:29
 */
public class MessageDeSerializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> stringStringConsumerRecord) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        //返回ConsumerRecord
        String key = null;
        String value = null;
        if (record.key() != null) {
            key = new String(record.key());
        }

        if (record.value() != null) {
            value = new String(record.value());
        }
        return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                key,
                value);
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }

}
