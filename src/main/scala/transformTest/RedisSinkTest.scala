package transformTest

import day02.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局并行度为1
    val inputStream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")
    val dataStream = inputStream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble)
    })

    //创建连接
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("slave1")
      .setPort(6379)
      .build()
    //Sink
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper()))


    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器id和温度值保存成hash表 HEST key field Value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")

  }

  //定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = {
    t.id //会根据hash值保存，所以一样的值只能保存一个
  }

  //定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }

}
