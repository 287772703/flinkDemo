package transformTest

import day02.SensorReading
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Process_Function {
  def main(args: Array[String]): Unit = {
    //初始化

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
     val stream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")

    //包装数据
    val dataStream = stream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble)
    })

      dataStream.keyBy(_.id)
      .process( new MyProcess()  )
  }
}

class MyProcess()extends KeyedProcessFunction[String,SensorReading,String]{
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(2000)  //注册timer Event  process

  }
}
