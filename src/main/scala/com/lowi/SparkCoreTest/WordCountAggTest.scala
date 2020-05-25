package com.lowi.SparkCoreTest

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 调优
  * 两阶段聚合（局部聚合+全局聚合）
  */
object WordCountAggTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountAggTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val array = Array("you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you",
      "you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you",
      "you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you",
      "you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you",
      "you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you","you you",
      "jump jump")

    val rdd = sc.parallelize(array, 8)
    rdd.flatMap(line => line.split(" "))
      .map(word => {
        val prefix = new Random().nextInt()
        (prefix + "_" + word, 1)
      }).reduceByKey(_ + _)
      .map(wc =>{
        val newWord = wc._1.split("_")(1)
        val count = wc._2
        (newWord, count)
      }).reduceByKey(_+_)
      .foreach(wc=>{
        println("单词" + wc._1 + " 次数：" + wc._2)
      })
  }
}
