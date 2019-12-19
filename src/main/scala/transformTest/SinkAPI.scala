package transformTest

import day02.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011



object SinkAPI {
  def main(args: Array[String]): Unit = {
    //Bahir 官方没有提供的，需要在自定义，比如redis
    //sink 官方提供了sink基本上不用自己去自定义
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局并行度为1

    val inputStream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")

    val dataStream = inputStream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble).toString
    }) //转成string方便序列化输出


    //sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("master:9092", "sinkTest", new SimpleStringSchema()))

    env.execute("kafka sink teat")


  }
}
