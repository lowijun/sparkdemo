package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cogroup和groupByKey的区别
  */
object CogroupAndGroupByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("CogroupAndGroupByKeyTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //cogroup
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    rdd3.foreach(println)
    println("=========================================")
    //groupByKey
    val rdd4: RDD[(String, Iterable[Int])] = rdd1.union(rdd2).groupByKey()
    rdd4.foreach(println)

    /**
      * 结果
      * cogroup
      * (tom,(CompactBuffer(1, 2),CompactBuffer(1)))
      * (kitty,(CompactBuffer(2),CompactBuffer()))
      * (jerry,(CompactBuffer(3),CompactBuffer(2)))
      * (shuke,(CompactBuffer(),CompactBuffer(2)))
      *
      * groupbykey
      * (kitty,CompactBuffer(2))
      * (tom,CompactBuffer(1, 2, 1))
      * (jerry,CompactBuffer(3, 2))
      * (shuke,CompactBuffer(2))
      *
      */
    sc.stop()
  }
}
