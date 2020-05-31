package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *flatMapValues类似于mapValues，不同的在于flatMapValues应用于元素为KV对的RDD中Value。
  * 每个一元素的Value被输入函数映射为一系列的值，然后这些值再与原RDD中的Key组成一系列新的KV对。
  */
object FlatMapValuesTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FlatMapValuesTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(5,6)))
    rdd1.flatMapValues(x=> 1.to(x)).collect().foreach(println)

    /**
      * (1,1) (1,2)
      * (3,1) (3,2) (3,3) (3,4)
      * (5,1) (5,2) (5,3) (5,4) (5,5) (5,6)
      */
    sc.stop()
  }
}
