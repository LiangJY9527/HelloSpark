package cn.itcast.spark.day4

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import scala.reflect.internal.util.TableDef.Column

//spark 2.0 之后的sparkSession
object SqlHdfs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SqlHdfs").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf)
//      .enableHiveSupport()//使用hive时需要
      .getOrCreate()
    import spark.implicits._
    val value: Dataset[(String, String, String)] = spark.read.textFile("hdfs://192.168.217.81:9000/sparkSql/user/").map(a => {
      val strings: Array[String] = a.split(" ")
      (strings(0), strings(1), strings(2))
    })

    val frame: DataFrame = value.toDF("id", "name", "age")
    frame.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
/*      .map(a =>{
      val files = a.split(" ")
      (files(0),files(1),files(2))
    }).rdd.sortBy(_._3)

    println(rdd.collect().toBuffer)*/
    
/*    val df = spark.read.json("hdfs://192.168.217.81:9000/sparkSql/jsonOut/part-*")
    df.createOrReplaceTempView("t_person")
    df.show()*/
    spark.close()
  }
}
