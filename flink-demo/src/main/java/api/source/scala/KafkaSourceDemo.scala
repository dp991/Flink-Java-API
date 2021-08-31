package api.source.scala

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @description ${Description}
 * @author zdp
 * @email 13221018869@189.cn
 * @date 2021/5/26 15:21
 */
object KafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.191.3:9092")
    //创建consumer
    val consumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)
    val stream = env.addSource(consumer)
    val rs = stream.flatMap(line => line.split(" ")).map(x => (x,1))
    rs.print()
    env.execute()
  }
}
