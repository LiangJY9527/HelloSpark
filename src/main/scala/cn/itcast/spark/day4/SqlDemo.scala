package cn.itcast.spark.day4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//sparkSql案例一：从hdfs读文件并以json格式写到hdfs中
/**
    将程序打成jar包，上传到spark集群，提交Spark任务
    /usr/local/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
    --class cn.itcast.spark.sql.InferringSchema \
    --master spark://node1.itcast.cn:7077 \
    /root/spark-mvn-1.0-SNAPSHOT.jar \
    hdfs://node1.itcast.cn:9000/person.txt \
    hdfs://node1.itcast.cn:9000/out
  */
object SqlDemo {
  def main(args: Array[String]): Unit = {
    //本地写入hdfs文件要设置用户名
    System.setProperty("HADOOP_USER_NAME","root")
    val conf = new SparkConf().setAppName("SqlDemo").setMaster("local[2]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    //spark 2.1.0 中过时
    val sqlContext = new SQLContext(sc)

    val rdd1 = sc.textFile("hdfs://192.168.217.81:9000/sparkSql/user/").map(_.split(" "))
    //创建case class
    //将RDD和case class关联
    val personRdd = rdd1.map(a =>Person(a(0).toInt,a(1),a(2).toInt))
    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRdd.toDF()
    //注册表
    //spark 2.1.0 中过时
    personDF.registerTempTable("t_person")
    val sqlDf = sqlContext.sql("select * from t_person where age >= 28 order by age desc limit 2")
    //转化为json存到hdfs中
    sqlDf.write.json("hdfs://192.168.217.81:9000/sparkSql/jsonOut")
    //打印
//    sqlDf.show()
    sc.stop()
  }
}
case class Person(id: Int,name: String,age: Int)