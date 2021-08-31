package api.source.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @description ${Description}
 * @author zdp
 * @email 13221018869@189.cn
 * @date 2021/5/26 10:10
 */
object LocalFileDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val fileStream = env.readTextFile("D:\\work\\projects\\flink-demo\\src\\main\\resources\\adclick.txt")
    fileStream.print()
    env.execute("read file from local by scala")
  }
}
