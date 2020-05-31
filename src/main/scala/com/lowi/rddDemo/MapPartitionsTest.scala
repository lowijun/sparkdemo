package com.lowi.rddDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MapPartitionsTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val data: RDD[Int] = sc.parallelize(1 to 9, 3)

    /**
      * mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的
      *
      * * def mapPartitions[U: ClassTag]
      * *     (f: Iterator[T] => Iterator[U],
      * *     preservesPartitioning: Boolean = false): RDD[U]
      */
    data.mapPartitions(myfunction).collect().foreach(println)

    sc.stop()
  }

  def myfunction[T] (iter: Iterator[T]) : Iterator[(T, T)] = {
    var res: List[(T, T)] = List[(T,T)]()
    var pre: T = iter.next()
    while (iter.hasNext) {
      val cur: T = iter.next()
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }
}
