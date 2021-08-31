package api.source.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @description ${Description}
 * @author zdp
 * @email 13221018869@189.cn
 * @date 2021/5/26 11:09
 */
object HadoopFileDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val streamFromDFS = env.readTextFile("hdfs://192.168.191.3:9000/in/adclick.txt")

    streamFromDFS.print()

    env.execute()
  }
}
