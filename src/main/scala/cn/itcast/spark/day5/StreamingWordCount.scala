package cn.itcast.spark.day5

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    //创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val context: SparkContext = new SparkContext(conf)
    //设置spark日志级别
    context.setLogLevel("ERROR")
    val ssc: StreamingContext = new StreamingContext(context,Seconds(5))
    //接收数据 测试：可以通过netcat发送数据：nc -lk 8888
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.217.81",8888)
    //DStream 是一个特殊的RDD
    //仅会记录当前批次的数据的数量，之前的数据不会累计
    val result: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //打印结果
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
