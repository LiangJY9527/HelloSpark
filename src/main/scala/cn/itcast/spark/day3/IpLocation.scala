package cn.itcast.spark.day3

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val mid = (low + high) / 2
      if (ip >= lines(mid)._1 && ip <= lines(mid)._2) {
        return mid
      }
      if (ip < lines(mid)._1) {
        high = mid - 1
      }
      if (ip > lines(mid)._2) {
        low = mid + 1
      }
    }
    -1
  }

  //spark写入mysql
  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into local_info (location, counts, accesse_date) value (?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.217.81:3306/test","root","hadoop")
      it.foreach(line =>{
        ps.setString(1,line._1)
        ps.setInt(2,line._2)
        ps.setDate(3,new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    }catch {
      case e: Exception => println("mysql Exception")
    }finally {
      if (ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //全部的ip映射规则
    val ipRuleRdd = sc.textFile("D:\\ztest\\ipRule\\").map(line => {
      val files = line.split("\\|")
      val ipStart = files(2).toLong
      val ipEnd = files(3).toLong
      val prov = files(6)
      (ipStart, ipEnd, prov)
    }).collect()
    //广播规则，可以把数据加载到每台子节点上，避免网络通信，快
    val brodRule = sc.broadcast(ipRuleRdd)

    val ipsRdd = sc.textFile("D:\\ztest\\ip\\").map(line => {
      val files = line.split("\\|")
      val ip = files(1)
      val ip10 = ip2Long(ip)
      val index = binarySearch(brodRule.value, ip10)
      val rule = brodRule.value(index)
      (rule._3, 1)
    })
    //根据省份聚合
    val provContRdd = ipsRdd.reduceByKey(_ + _)
    //写入mysql数据库中
    provContRdd.foreachPartition(data2MySQL)

    println(provContRdd.collect().toBuffer)
  }

}
