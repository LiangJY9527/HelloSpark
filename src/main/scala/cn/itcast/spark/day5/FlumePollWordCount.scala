package cn.itcast.spark.day5

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//spark从flume拉取数据：可能会用到
object FlumePollWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    //从flume中拉取数据（flume的地址）
    val addresses: Seq[InetSocketAddress] = Seq(new InetSocketAddress("192.168.217.81",8888))
    val rds: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,addresses,StorageLevel.MEMORY_AND_DISK)
    val ds: DStream[(String, Int)] = rds.flatMap(x=>new String(x.event.getBody.array()).split(" ")).map((_,1))
    val result: DStream[(String, Int)] = ds.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
