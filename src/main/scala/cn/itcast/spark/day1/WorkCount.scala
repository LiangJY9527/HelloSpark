package cn.itcast.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

object WorkCount {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("wc")
//    val config = new SparkConf().setAppName("wc").setJars(Array("")).setMaster("spark://hadoop01:7077")
    val sc = new SparkContext(config)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
    sc.stop()

    //wordcount产生的RDD数量：
    //textFile会产生两个RDD:HadoopRDD -> MapPartitionsRDD
    //sc.textFile(args(0)).
    //产生一个RDD:MapPartitionsRDD
    // flatMap(_.split(" ")).
    //产生一个RDD:MapPartitionsRDD
    // map((_,1)).
    //产生一个RDD:ShuffledRDD
    // reduceByKey(_+_).
    //产生一个RDD:MapPartitionsRDD
    // saveAsTextFile(args(1))
  }
}
