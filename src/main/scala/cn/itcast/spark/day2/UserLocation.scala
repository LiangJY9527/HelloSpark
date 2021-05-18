package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}
//获取每个用户停留时间最长的两个基站
object UserLocation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Users\\IdeaProjects\\BigData\\hadoop-2.6.5")
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:/ztest/lac/").map(line =>{
      val files = line.split(",")
      val phone = files(0)
      val times = files(1)
      val lac = files(2)
      val timeLong = if (files(3) == "1") -times.toLong else times.toLong
      ((phone,lac),timeLong)
    })
//    val rdd2 = rdd1.groupByKey().mapValues(_.sum).map(x => (x._1._1,x._1._2,x._2)).groupBy(_._1)
//    val rdd3 = rdd2.mapValues(_.toList.sortBy(_._3).reverse.take(2))

    val rdd2 = rdd1.reduceByKey(_+_).map(x =>(x._1._2,(x._1._1,x._2)))
    val rdd3 = sc.textFile("D:\\ztest\\jz\\").map(line =>{
      val files = line.split(",")
      (files(0),(files(1),files(2)))
    })
    val rdd4 = rdd2.join(rdd3).map(x=>{
      val phone = x._2._1._1
      val lac = x._1
      val times = x._2._1._2
      val a = x._2._2._1
      val b = x._2._2._2
      (phone,lac,a,b,times)
    })
    val rdd5 = rdd4.groupBy(_._1).mapValues(_.toList.sortBy(_._5).reverse.take(2))
//ArrayBuffer((18688888888,List((18688888888,16030401EAFB68F1E3CDF819735E1C66,116.296302,40.032296,87600), (18688888888,9F36407EAD0629FC166F14DDE7970F68,116.304864,40.050645,51200))), (18611132889,List((18611132889,16030401EAFB68F1E3CDF819735E1C66,116.296302,40.032296,97500), (18611132889,9F36407EAD0629FC166F14DDE7970F68,116.304864,40.050645,54000))))
    println(rdd5.collect().toBuffer)
//    rdd5.saveAsTextFile("D:\\ztest\\out\\")
    sc.stop()
  }

}
