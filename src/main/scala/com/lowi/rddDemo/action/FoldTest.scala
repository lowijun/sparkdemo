package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def fold(zeroValue: T)(op: (T, T) ⇒ T): T
  * fold是aggregate的简化，
  * 将aggregate中的seqOp和combOp使用同一个函数op。
  */
object FoldTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TakeOrderedTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //    val rdd1: RDD[Int] = sc.parallelize(Seq(10,4,2,12,3))
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10,2)
    val i: Int = rdd1.fold(1)(
      (x, y) => x + y
    )
    println(i)

    /**
      * 结果
      * Int = 58
      *
      * 结果为什么是58，看下面的计算过程：
      * ##先在每个分区中迭代执行 (x : Int,y : Int) => x + y 并且使用zeroValue的值1
      * ##即：part_0中 zeroValue+5+4+3+2+1  = 1+5+4+3+2+1  = 16
      * ##    part_1中 zeroValue+10+9+8+7+6 = 1+10+9+8+7+6 = 41
      * ##再将两个分区的结果合并(x : Int,y : Int) => x + y ，并且使用zeroValue的值1
      * ##即：zeroValue+part_0+part_1 = 1 + 16 + 41 = 58
      */

    sc.stop()
  }

}
