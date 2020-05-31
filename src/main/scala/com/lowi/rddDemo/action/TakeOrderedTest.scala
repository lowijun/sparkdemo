package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
  * takeOrdered和top类似，只不过以和top相反的顺序返回元素。
  */
object TakeOrderedTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TakeOrderedTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //    val rdd1: RDD[Int] = sc.parallelize(Seq(10,4,2,12,3))
    val rdd1: RDD[Int] = sc.makeRDD(Seq(10,4,2,12,3))
    val array: Array[Int] = rdd1.top(1)
    array.foreach(println)
    /**
      * 结果
      * Array[Int] = Array(12)
      */
    val array2: Array[Int] = rdd1.top(2)
    array2.foreach(println)
    /**
      * 结果
      * Array[Int] = Array(12, 10)
      */

    rdd1.takeOrdered(1).foreach(println)
    /**
      * 结果
      * Array[Int] = Array(2)
      */
    rdd1.takeOrdered(2).foreach(println)
    /**
      * 结果
      * Array[Int] = Array(2,3)
      */
    sc.stop()
  }
}
