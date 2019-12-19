package day02

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//温度传感器
case class SensorReading(id:String,timestamp:Long,temperature:Double)
object DataStreamAPI {
  def main(args: Array[String]): Unit = {
    //构建执行环境
    //val env =StreamExecutionEnvironment.createLocalEnvironment(1) //本地环境

    val env = StreamExecutionEnvironment.getExecutionEnvironment

     //1.从自定义中读取
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 15732134, 43.454),
      SensorReading("sensor_1", 15732134, 45.454),
      SensorReading("sensor_2", 15732165, 44.434),
      SensorReading("sensor_3", 15732174, 42.414),
      SensorReading("sensor_4", 15732184, 41.451),
      SensorReading("sensor_5", 15732189, 43.454)))

    //stream1.print("stream1").setParallelism(1) //print就相当于sink

    //2.从文件中读取
    val stream2 = env.readTextFile("G:\\flinkDemo\\data\\words")
    //stream2.print().setParallelism(1)


    env.execute()
  }
}
