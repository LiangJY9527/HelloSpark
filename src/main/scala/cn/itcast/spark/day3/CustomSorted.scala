package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object MyPrefer{
  implicit def grilToOrdering = new Ordering[Gril](){
    override def compare(x: Gril, y: Gril): Int = {
      if (x.faceValue == y.faceValue) y.age -x.age
      else x.faceValue - y.faceValue
    }
  }
}
//自定义排序
object CustomSorted {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("CustomSorted").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("liuyf",90,25,1),("anglebaby",90,30,2),("hanikezi",95,22,3)))
    import MyPrefer._
    val rdd2 = rdd1.sortBy(g =>Gril(g._2,g._3),false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}
//方法一：
/*
case class Gril(faceValue: Int,age: Int) extends Ordered[Gril] with Serializable {
  override def compare(that: Gril): Int = {
    if (this.faceValue == that.faceValue) that.age -this.age
    else this.faceValue - that.faceValue
  }
}
*/
//方法二：
case class Gril(faceValue: Int,age: Int) extends Serializable