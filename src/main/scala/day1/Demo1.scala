package day1

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object Demo1 {
  def main(args: Array[String]): Unit = {
    //构造执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val input = "G:\\flinkDemo\\data\\words"
    val ds: DataSet[String] = env.readTextFile(input)
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    //经过groupby进行分组，sum进行聚合
    val aggDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // 打印
    aggDs.print()

  }
}
