package transformTest


import day02.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowAPI {
  def main(args: Array[String]): Unit = {

    //初始化

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
   // val stream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")
   val stream =env.socketTextStream("master",7777)
    //包装数据
    val dataStream = stream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble)
    })
      .assignAscendingTimestamps(_.timestamp*1000)

    //统计10s内的最小温度
    val minTempPertureWindowStream =dataStream.map(data=>(data.id,data.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(10)) //开时间窗口
        .reduce((data1,data2)=>(data1._1,data1._2.min(data2._2)))  //reduce做增量聚合

    minTempPertureWindowStream.print()

    env.execute("window test")
  }
}
