package transformTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import day02.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //全局并行度为1

    //source
    val inputStream = env.readTextFile("G:\\flinkDemo\\data\\SensorReading")

    //transform
    val dataStream = inputStream.map(lines => {
      val line = lines.split(" ")
      SensorReading(line(0).trim, line(1).trim.toLong, line(2).trim.toDouble)
    })

    //sink
    dataStream.addSink(new MyJdbcSink())

    env.execute()
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  //定义sql的连接、预编译器
  var conn: Connection = _

  var insertStmt: PreparedStatement = _

  var updateStmt: PreparedStatement = _

  //创建连接，和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://slave2:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures(sensor,temp) VALUE(?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp =? WHERE sensor =?")
  }

  //调用连接 执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果update没有查到数据执行插入
    if (updateStmt.getLargeUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}
