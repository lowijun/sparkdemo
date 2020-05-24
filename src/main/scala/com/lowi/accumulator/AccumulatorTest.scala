package com.lowi.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    //1、构建sparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("accumulator")
    //2.构建sparkcontext
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("warn")
    //创建accumulator并初始化0
    val accumulator: LongAccumulator = sc.longAccumulator
//    val accumulator = sc.accumulator(0)
    //读取文件数据
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\words.txt")
    val result = data.map(line => {
      accumulator.add(1)
      line
    })

    //触发action操作
    result.collect()
    println("words lines is " + accumulator.value)
    sc.stop()

  }

}
