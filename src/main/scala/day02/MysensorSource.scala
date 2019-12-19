package day02

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object MysensorSource {
  def main(args: Array[String]): Unit = {
    //一般用在测试环境，在生产环境中用的比较少

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","master:9092")
    properties.setProperty("group.id","consumer.group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    //自定义source
    val stream = env.addSource(new SensorSource())
    stream.print("stream").setParallelism(1)

    env.execute()
  }
}
class SensorSource() extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据是否运行
  var running:Boolean = true
  var flag =0
  //正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
   //初始化一个随机数生成器
    val rand = new Random()

    // 初始化定义一组传感器数据
    var  curTemp =1.to(10).map(
      i=>("sensor_"+i,60+rand.nextGaussian()*20) // nextGaussian()高斯随机数  、正太分布随机选取
    )
    //产生无限数据流
    while (running){
      //数更新在前一次温度的基础上更新温度值
      curTemp.map(
        t=>(t._1,t._2+rand.nextGaussian())
      )
      //获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(t=>
        ctx.collect(SensorReading(t._1,curTime,t._2) )//用ctx把当前包装好的数据发出去
      )
      Thread.sleep(1000) //观察时间
      flag+=1
      println(s"**************************$flag*************************")
    }
  }
 //取消数据源 的生成
  override def cancel(): Unit = {
    running =false
  }
}
