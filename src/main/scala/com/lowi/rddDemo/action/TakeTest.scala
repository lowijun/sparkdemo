package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def take(num: Int): Array[T]
  * take用于获取RDD中从0到num-1下标的元素，不排序。
  */
object TakeTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TakeTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
//    val rdd1: RDD[Int] = sc.parallelize(Seq(10,4,2,12,3))
    val rdd1: RDD[Int] = sc.makeRDD(Seq(10,4,2,12,3))
    val array: Array[Int] = rdd1.take(1)
    array.foreach(println)
    /**
      * 结果
      * Array[Int] = Array(10)
      */
    val array2: Array[Int] = rdd1.take(2)
    array2.foreach(println)
    /**
      * 结果
      * Array[Int] = Array(10, 4)
      */
    sc.stop()
  }
}
