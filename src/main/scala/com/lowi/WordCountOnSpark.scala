package com.lowi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo  利用Scala语言开发spark程序实现单词统计
object WordCountOnSpark {
  def main(args: Array[String]): Unit = {
    //构建sparkConf对象 设置application名称
    val sparkConf:SparkConf = new SparkConf().setAppName("wordCountOnSpark")
    //构建sparkContext对象，该对象是所有spark程序的入口
    //它内部会创建2个对象：DAGScheduler和TaskScheduler
    val sc = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("warn")
    //读取文件数据
    val data:RDD[String] = sc.textFile(args(0))
    //切分每一行，获取所有单词
    val words:RDD[String] = data.flatMap(_.split(""))
    //每个单词记为1
    val wordOne = words.map((_,1))
    //相同单词累加1
    val result = wordOne.reduceByKey(_+_)
    //将结果保存在hdfs上
    result.saveAsTextFile(args(1))
    //关闭
    sc.stop()
  }
}
