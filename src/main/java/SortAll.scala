import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

object SortAll {
  def main(args: Array[String]): Unit = {
    val timestart = System.currentTimeMillis()

    val num=100000
    val predata = new ArrayBuffer[Int]
    for (i <- 1 to 10000) {
      predata += Random.nextInt(num)
    }
    for(i<-predata.indices)
      {
        println(i)
      }
    val sorteddata = SinsrtSort(predata)  //直接插入排序 The runtime is  0.307  s

//    for (i <- sorteddata.indices) {
//      println(i)
//  }
    val timeover = System.currentTimeMillis()
    print("The runtime is  " + (timeover - timestart) / 1000.0 + "  s")

  }
  /*函数功能：直接插入排序*/
  def SinsrtSort(inputData: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    for (i <- 1 until inputData.length) {
      val x = inputData(i)
      var j = i - 1
      while (j > 0 && x < inputData(j)) {
        inputData(j + 1) = inputData(j)
        j = j - 1
      }
      inputData(j + 1) = x
    }
    inputData
  }
}
