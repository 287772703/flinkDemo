package transformTest

import day02.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011




object kafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局并行度为1

    //source
    val inputStream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")

    //transform
    val dataStream = inputStream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble).toString
    })

    //sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("master:9092","sensor",new SimpleStringSchema()))

    env.execute()
  }
}
