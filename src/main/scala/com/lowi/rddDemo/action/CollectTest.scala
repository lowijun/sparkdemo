package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def collect(): Array[T]
  * collect用于将一个RDD转换成数组。
  */
object CollectTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CollectTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10,2)
    val arr: Array[Int] = rdd1.collect()
    arr.foreach(println)
    /**
      * 结果
      * Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      */
    sc.stop()
  }
}
