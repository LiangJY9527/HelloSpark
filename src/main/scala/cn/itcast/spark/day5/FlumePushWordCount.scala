package cn.itcast.spark.day5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//flume往一个spark推数据：不常用
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FlumePushWordCount").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    //推送方式：flume向spark推送数据
  val rds: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,"192.168.217.20",8888)
    //flume中的数据通过event.getBody才能真正拿到
    val ds: DStream[(String, Int)] = rds.flatMap(x =>new String(x.event.getBody.array()).split(" ")).map((_,1))
    val result: DStream[(String, Int)] = ds.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
