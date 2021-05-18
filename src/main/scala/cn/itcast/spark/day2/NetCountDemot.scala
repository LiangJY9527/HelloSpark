package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
//自定义分区
object NetCountDemot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetCountDemot").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\ztest\\net\\").map(line =>{
      val files = line.split("\t")
      (files(1),1)
    })
    //countByKey()得到的类型为map（是在driver端的最终结果），而reduceByKey()得到的类型是RDD
    val rdd2 = rdd1.reduceByKey(_+_).map(x =>{
      val url = x._1
      val host = new URL(url).getHost
      (host,(url,x._2))
    })
    val ins = rdd2.map(_._1).distinct.collect()
    val netPartitions = new NetPartitions(ins)
    //分区之后，遍历需要用mapPartitions
    val rdd3 = rdd2.partitionBy(netPartitions).mapPartitions(it =>{
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    rdd3.saveAsTextFile("D:\\ztest\\netOut\\")
    println(rdd3.collect().toBuffer)
    sc.stop()
  }
}

class NetPartitions(ins: Array[String]) extends Partitioner {
  val parMap = new mutable.HashMap[String,Int]()
  var count = 0
  for (i <- ins){
    parMap += (i->count)
    count += 1
  }
  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}