package cn.itcast.spark.day3

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
//从mysql数据库读取数据
object JdbcRddReadDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRddReadDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.217.81:3306/test","root","hadoop")
    }

    val jdbcRdd = new JdbcRDD(
      sc,
      connection,
      "select * from user where id>=? and id<=?",
      1,
      3,
      2,
      rs =>{
        val id = rs.getInt(1)
        val code = rs.getString(2)
        (id,code)
    })
    val result = jdbcRdd.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
