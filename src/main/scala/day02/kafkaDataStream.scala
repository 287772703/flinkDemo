package day02

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object kafkaDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从kafka读取数据
    //flink可以将offset当作状态保存下来 自动保证状态的一致性，什么都不用管
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","master:9092")
    properties.setProperty("group.id","consumer.group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

      val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

      stream.print("steam").setParallelism(1)
     env.execute()
  }
}

