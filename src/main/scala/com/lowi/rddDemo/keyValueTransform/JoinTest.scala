package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * join
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("JoinTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    val arr: Array[(String, (Int, Int))] = rdd3.collect()
    arr.foreach(println)
    /**
      * 结果
      * (tom,(1,1))
      * (jerry,(3,2))
      */
    sc.stop()
  }
}
