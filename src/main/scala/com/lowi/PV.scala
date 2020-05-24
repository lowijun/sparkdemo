package com.lowi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo 利用spark实现点击流日志分析-----PV
object PV {
  def main(args: Array[String]): Unit = {
    //1.构建SparkConf
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2.构建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.读取数据文件
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\access.log")
    //4.统计pv
    val pv: Long = data.count()
    print("pv:"+ pv)

    sc.stop()
  }
}
