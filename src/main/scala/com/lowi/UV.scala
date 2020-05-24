package com.lowi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo 利用spark实现点击日志分析---uv
object UV {
  def main(args: Array[String]): Unit = {
    //1.构建sparkConf
    val sparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    //2.构建sparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3。读取数据文件
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\access.log")
    //4.切分每一行，获取每一个元素，也就是ip
    val ips: RDD[String] = data.map(x=>x.split(" ")(0))
    //按照ip去重
    val distinctRDD: RDD[String] = ips.distinct()
    //统计uv
    val uv: Long = distinctRDD.count()
    println("uv:"+uv)
    sc.stop()

  }
}
