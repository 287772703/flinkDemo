package day1

import org.apache.flink.api.scala.ExecutionEnvironment
import  org.apache.flink.api.scala._
object flinkWc {
  def main(args: Array[String]): Unit = {
    //批处理的wordcount

    //创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputpath="G:\\flinkDemo\\data\\words"
    val inputData =env.readTextFile(inputpath)

   //切分数据

    val wordCount =inputData.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    wordCount.writeAsCsv("G:\\flinkDemo\\data\\word.csv","\n"," ").setParallelism(2)
    env.execute("wordcount")
  }
}
