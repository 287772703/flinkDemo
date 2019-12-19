package transformTest
import  org.apache.flink.api.scala._
import day02.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SplitDemo {
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

//      .reduce((x,y)=>SensorReading(x.id,x.timestamp*1000,y.temperature))
//      .print()

    env.execute("reduce")

  }
}
