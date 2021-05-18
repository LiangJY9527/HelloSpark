package cn.itcast.spark.day5

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object StateFulWordCount {
  //String：单词
  //Seq[Int]：这个批次某个单词出现的次数的集合
  //Option[Int]：这个批次这个单词的以前的次数
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])] ) =>{
       /*iter.map(t =>{
         (t._1,t._2.sum+t._3.getOrElse(0))
       })*/
/*    iter.flatMap {
      case (x, y, z) =>
        Some(x, y.sum + z.getOrElse(0))
    }*/
    iter.map {
      case (x, y, z) =>
        (x, y.sum + z.getOrElse(0))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    //设置日志级别
    sc.setLogLevel("ERROR")
    //使用updateStateByKey方法必须设置setCheckpointDir
    sc.setCheckpointDir("D:\\ztest\\stream\\")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    val rsd: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.217.81", 8888)

    val ds: DStream[(String, Int)] = rsd.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultMinPartitions), true)
    ds.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
