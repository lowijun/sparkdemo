package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
  * def foreach(f: (T) ⇒ Unit): Unit
  *
  * foreach用于遍历RDD,将函数f应用于每一个元素。
  * 但要注意，如果对RDD执行foreach，只会在Executor端有效，而并不是Driver端。
  * 比如：rdd.foreach(println)，只会在Executor的stdout中打印出来，Driver端是看不到的。
  * 我在Spark1.4中是这样，不知道是否真如此。
  * 这时候，使用accumulator共享变量与foreach结合，倒是个不错的选择。
  */
object ForeachTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    var rdd1 = sc.makeRDD(1 to 10,2)
    var accumulator: LongAccumulator = sc.longAccumulator
    rdd1.foreach(x=>accumulator.add(x))
    println(accumulator.value)
    /**
      * 结果
      * Int = 55
      */

    sc.stop()
  }
}
