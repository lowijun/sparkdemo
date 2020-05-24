package com.lowi.test20200523

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //构建sparkconf对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wordCount")
    //构建sparkcontext
    val sc = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("warn")
    //获取数据源数据
    val data: RDD[String] = sc.textFile("E:\\wordCount.txt")

    //处理数据
    val wordCount: RDD[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //结果进行排序
    val wordCountAce: RDD[(String, Int)] = wordCount.sortBy(_._2, true)
    //打印
    wordCountAce.collect().foreach(println)
    //关闭
    sc.stop()
  }
}
