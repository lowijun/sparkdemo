package com.lowi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo 利用spark实现点击日志分析-----topn（求页面访问次数最多的前N位）
object TopN {
  def main(args: Array[String]): Unit = {
    //1.构建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    //2.构建sparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.读取数据文件
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\access.log")
    //4.切分每一行，过滤掉丢失的字段数据，获取页面地址
    val filterRDD: RDD[String] = data.filter(_.split(" ").length > 10)
    val urlAndOne: RDD[(String, Int)] = filterRDD.map(_.split(" ")(10)).map((_,1))

    //5.相同url出现的1累加
    val result = urlAndOne.reduceByKey(_+_)
    //6.按照次数降序
    val sortedRDD: RDD[(String, Int)] = result.sortBy(_._2,false)
    //7.取出url出现次数最多的前5位
    val top5: Array[(String, Int)] = sortedRDD.take(5)
    top5.foreach(println)
    sc.stop()
  }
}
