package cn.itcast.spark.day4

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo2 {
  //sparkSql案例二：从hdfs读文件并以json格式写到hdfs中
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SqlDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //spark 2.1.0 中过时
    val sqlContext = new SQLContext(sc)
    val rdd1 = sc.textFile("hdfs://192.168.217.81:9000/sparkSql/user/").map(_.split(" "))

    val schema = StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    ))

    val rowRdd = rdd1.map(a => Row(a(0).toInt,a(1),a(2).toInt))
    val personDf = sqlContext.createDataFrame(rowRdd,schema)
    personDf.registerTempTable("t_person")
    val sqlDf = sqlContext.sql("select * from t_person")
    sqlDf.show()
    sc.stop()
  }
}
