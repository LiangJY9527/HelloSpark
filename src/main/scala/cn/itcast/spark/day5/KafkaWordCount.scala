package cn.itcast.spark.day5

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

  def updateFunc = (it: Iterator[(String,Seq[Int],Option[Int])])=>{
    it.map{
      case (x,y,z) =>(x,y.sum+z.getOrElse(0))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("D:\\ztest\\kafka\\")
    var topicMap = new scala.collection.immutable.HashMap[String,Int]()
    topicMap += ("test"->2)
    val rds: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,"192.168.217.81:2181","g1",topicMap,StorageLevel.MEMORY_ONLY_SER)
    val words: DStream[String] = rds.map(_._2).flatMap(_.split(" "))
    val result: DStream[(String, Int)] = words.map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultMinPartitions),true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
