package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\Users\\IdeaProjects\\BigData\\hadoop-2.6.1")
    val conf =new SparkConf().setAppName("foreachDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\ztest\\lac\\DDE7970F68").map(line =>{
      val files = line.split(",")
      val phone = files(0)
      val times = files(1)
      val lac = files(2)
      val timeLong = if (files(3) == "1") -times.toLong else times.toLong
      ((phone,lac),timeLong)
    })
    println(rdd1.collect().toBuffer)
      sc.stop()
  }

}
