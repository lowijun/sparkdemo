package com.lowi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //构建sparkconf对象 设置application名称和master地址
    val sparkConf:SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.3")
    //构建spark Context对象，该对象非常重要，它是所有spark程序的执行入口
    //它内部会构建  DAGScheduler和 TaskScheduler对象
    val sc= new SparkContext(sparkConf)

    //设置日志级别
    sc.setLogLevel("warn")
    //读取数据文件
    val data:RDD[String] = sc.textFile("E:\\wordCount.txt")
    //切分每一行，获取所有单词
    val words:RDD[String] = data.flatMap(_.split(" "))
    //每个单词记为1
    val wordOne:RDD[(String,Int)] = words.map((_ , 1))

    //相同的单词出现累加1
    val result:RDD[(String,Int)] = wordOne.reduceByKey(_+_)
    //按照单词出现的次数降序排序 第二个参数默认是true升序，设置为false表示降序
    val sortedRDD = result.sortBy(x => x._2, false)
    //收集数据打印
    val finalResult = sortedRDD.collect()
    finalResult.foreach(println)
    //关闭sc
    sc.stop()
  }
}
