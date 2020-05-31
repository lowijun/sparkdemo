package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def count(): Long
  * count返回RDD中的元素数量。
  */
object CountTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CountTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    val l: Long = rdd1.count()
    println(l)

    /**
      * 结果
      * Long = 3
      */

    sc.stop()
  }
}
