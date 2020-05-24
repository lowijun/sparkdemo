package com.lowi.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestBroadcastMain {
  def main(args: Array[String]): Unit = {
    //1.构建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("TestBroadcastMain").setMaster("local[2]")
    //2.构建sparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.构建数据源
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\words.txt")
    //通过调用sparkContext对象的broadcast方法将数据广播出去
    val word = "spark"
    val broadcast: Broadcast[String] = sc.broadcast(word)

    //在excutor中通过调用广播变量value属性获取广播变量的值
    val rdd2: RDD[String] = data.flatMap(_.split(" ")).filter(x => x.equals(broadcast.value))
    rdd2.foreach(x => println(x))
    sc.stop()

  }

}
