package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def countByKey(): Map[K, Long]
  *
  * countByKey用于统计RDD[K,V]中每个K的数量。
  */
object CountByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CountByKeyTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //    val rdd1: RDD[Int] = sc.parallelize(Seq(10,4,2,12,3))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("B",1),("C",1)))
    val map: collection.Map[String, Long] = rdd1.countByKey()
    //    seq1.foreach(println)
    map.foreach(println)
    /**
      * 结果
      * scala.collection.Map[String,Long] = Map(A -> 2, B -> 3, C-> 1)
      */

    sc.stop()
  }
}
