package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def lookup(key: K): Seq[V]
  *
  * lookup用于(K,V)类型的RDD,指定K值，返回RDD中该K对应的所有V值。
  */
object LookupTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("LookupTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //    val rdd1: RDD[Int] = sc.parallelize(Seq(10,4,2,12,3))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    val seq1: Seq[Int] = rdd1.lookup("A")
//    seq1.foreach(println)
    println(seq1)
    /**
      * 结果
      * Seq[Int] = WrappedArray(0, 2)
      */
    val seq2: Seq[Int] = rdd1.lookup("B")
    println(seq2)
    /**
      * 结果
      * Seq[Int] = WrappedArray(1, 2)
      */
    sc.stop()
  }
}
