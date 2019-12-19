package day1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
object DStreamWcApp {

  def main(args: Array[String]): Unit = {
    //构造环境
     val env =StreamExecutionEnvironment.getExecutionEnvironment

    val tool = ParameterTool.fromArgs(args)

    val inputPath = tool.get("input")
    val outputPath = tool.get("output")

    val dataSteam = env.readTextFile(inputPath)

    val sumDStream = dataSteam.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_, 1)).keyBy(0).sum(1)
    sumDStream.writeAsCsv(outputPath).setParallelism(1)

    //从文件中读取
    val stream2 = env.readTextFile("G:\\flinkDemo\\data\\words")

    stream2.print().setParallelism(1)
    env.execute()
  }
}
