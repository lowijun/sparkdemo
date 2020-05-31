package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues顾名思义就是输入函数应用于RDD中Kev-Value的Value，
  * 原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。
  * 因此，该函数只适用于元素为KV对的RDD
  */
object MapValuesTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("MapValuesTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rdd2: RDD[(Int, String)] = rdd1.map(x => (x.length, x))
    rdd2.mapValues("x" +_+"x").collect().foreach(println)

    /**
      * 结果
      * Array[(Int, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx),(3,xcatx), (7,xpantherx), (5,xeaglex))
      */
    sc.stop()
  }
}
