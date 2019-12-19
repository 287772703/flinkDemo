package transformTest

import day02.SensorReading
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object tranform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /*
    map  flatMap  filter
    KeyBey  //分组分区  通过hash分区  DataDS => KeyedStream
  //滚动算子  必须在KeyedStram后使用 sum count reduce vag max
    */
    val dataStream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")

    val dataDS = dataStream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble)
    }).keyBy("id")





    // .sum(2)
    //当前最新的温度加+10，时间—+1
    //      .reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature+10.00))
    //        .print()

    //split  分流  并没有实际分开 现在使用out put
    val splitDS = dataDS.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitDS.select("high")
    val low = splitDS.select("low")
    val all = splitDS.select("high", "low")

    //connect
    // 把两条流合并到一块  只支持两条流合并 数据结构可以不一样
    val warning = high.map(data => (data.id, data.temperature))
    val connetStream = warning.connect(low)

    val coMapStream = connetStream.map(warndata => (warndata._1, warndata._2, "warning"),
      lowdata => (lowdata.id, "healthy")
    )
   // coMapStream.print()

    //union 可以合并多条流，数据结构必须一样
    val unionStream =high.union(low)
   //unionStream.print()

    //自定义filter函数类  很多函数提供了自定义的接口
    //函数类 可以直接传匿名函数，也可以给函数传参数
    //Rich Function 叫函数，其实不是函数，是类，可以获取运行函数的上下文
    //有生命周期的概念，几乎所有的函数都有rich版本
    //可以利用上下文进行连接hdfs，redis，mysql
    dataDS.filter(new Myfilter())
        .print()




    env.execute("tranform Test")
  }
}
//自定义filter类
class Myfilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}
