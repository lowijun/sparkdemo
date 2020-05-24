package com.lowi.SparkCoreTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    //构建sparkconf对象 设置application名称和master地址
    val sparkConf:SparkConf = new SparkConf().setAppName("AggregateByKeyTest").setMaster("local[2]")
    //构建spark Context对象，该对象非常重要，它是所有spark程序的执行入口
    //它内部会构建  DAGScheduler和 TaskScheduler对象
    val sc= new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("warn")
    //准备数据
    val pairRdd: RDD[(String, (String, Int))] = sc.parallelize(
      List(
        ("84.174.205.5",("2018-11-10 23:23:23",2)),
        ("221.226.113.146",("2018-09-11 23:23:23",3)),
        ("84.174.205.5",("2018-12-15 23:23:23",5)),
        ("108.198.168.20",("2018-01-03 23:23:23",2)),
        ("108.198.168.20",("2018-11-21 23:23:23",4)),
        ("221.226.113.146",("2018-11-01 23:23:23",6)),
        ("221.226.113.146",("2018-12-06 23:23:23",6))
      ),2)
    //运用aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
    //1、U定义为ArrayBuffer
    val juhe = pairRdd.aggregateByKey(ArrayBuffer[(String, Int)]())((arr,value)=>{
      //2、将value放入集合U中
      arr += value
      //3、将所有的集合进行合并
    },_.union(_))
    juhe.foreach(println(_))

    println("========================================================")

    val juhesum = juhe.mapPartitions(partition=>{
      partition.map(m=>{
        val key = m._1
        val date = m._2.map(m=>m._1).toList.sortWith(_<_)(0)
        val sum = m._2.map(m=>m._2).sum
        Row(key,date,sum)
      })
    })
    juhesum.foreach(println(_))
  }

}
