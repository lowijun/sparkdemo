package com.lowi.rddDemo.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * def first(): T
  * first返回RDD中的第一个元素，不排序。
  */
object FirstTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FirstTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(String, String)] = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    val tuple: (String, String) = rdd1.first()
    println(tuple)

    /**
      * 结果
      * (String, String) = (A,1)
      */
    val rdd2: RDD[Int] = sc.makeRDD(Seq(10, 4, 2, 12, 3))
    val i: Int = rdd2.first()
    println(i)
    /**
      * Int = 10
      */
    sc.stop()
  }
}
