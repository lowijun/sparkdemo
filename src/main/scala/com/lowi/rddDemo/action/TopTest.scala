package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def top(num: Int)(implicit ord: Ordering[T]): Array[T]
  *
  * top函数用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
  */
object TopTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TopTest")
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

    implicit val myOrd = implicitly[Ordering[Int]].reverse
    rdd1.top(1).foreach(println)
    /**
      * 结果
      * Array[Int] = Array(2)
      */
    rdd1.top(2).foreach(println)
    /**
      * 结果
      * Array[Int] = Array(2,3)
      */
    sc.stop()
  }
}
