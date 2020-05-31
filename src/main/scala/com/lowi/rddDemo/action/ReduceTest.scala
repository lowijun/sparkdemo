package com.lowi.rddDemo.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * def reduce(f: (T, T) ⇒ T): T
  * 根据映射函数f，对RDD中的元素进行二元计算，返回计算结果
  */
object ReduceTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ReduceTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10,2)
    val i: Int = rdd1.reduce(_+_)
    println(i)
    /**
      * 结果
      * Int = 55
      */
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    val tuple: (String, Int) = rdd2.reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    println(tuple)
    /**
      * 结果
      * (String, Int) = (CBBAA,6)
      */

    sc.stop()
  }
}
