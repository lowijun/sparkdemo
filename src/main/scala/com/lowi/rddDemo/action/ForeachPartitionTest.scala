package com.lowi.rddDemo.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.LongAccumulator

/**
  * def foreachPartition(f: (Iterator[T]) ⇒ Unit): Unit
  *
  * foreachPartition和foreach类似，只不过是对每一个分区使用f。
  */
object ForeachPartitionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    var rdd1 = sc.makeRDD(1 to 10,2)
    var accumulator: LongAccumulator = sc.longAccumulator
    rdd1.foreachPartition{
      x=>{
        accumulator.add(x.size)   //x.size表示迭代器中数据个数
      }
    }
    println(accumulator.value)
    /**
      * 结果
      * Int = 55
      */

    sc.stop()
  }
}
