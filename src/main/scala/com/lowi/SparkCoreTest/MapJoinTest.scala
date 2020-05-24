package com.lowi.SparkCoreTest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 调优
  * 将reduce join转为map join
  */
object MapJoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapJoinTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val lista = Array(
      Tuple2("001", "令狐冲"),
      Tuple2("002", "任盈盈")
    )
    //数据量小一点
    val listb=Array(
      Tuple2("001","一班"),
      Tuple2("002","二班")
    )
    val listaRDD = sc.parallelize(lista)
    val listbRDD = sc.parallelize(listb)

//    //reduce join方式
//    val result: RDD[(String, (String, String))] = listaRDD.join(listbRDD)
//    result.foreach(rdd =>{
//      println("班级号："+rdd._1+" 姓名："+rdd._2._1 + " 班级名：" + rdd._2._2)
//    })


    //map join方式
    //设置广播变量
    val listbBoradcast = sc.broadcast(listbRDD.collect())
    listaRDD.map(tuple => {
      val key = tuple._1
      val name = tuple._2
      val map = listbBoradcast.value.toMap
      val className = map.get(key)
      (key, (name,className))
    }).foreach(tuple =>{
      println("班级号："+tuple._1+" 姓名："+tuple._2._1 + " 班级名：" + tuple._2._2.get)
    })

    /**
      * 班级号：002 姓名：任盈盈 班级名：二班
      * 班级号：001 姓名：令狐冲 班级名：一班
      */
  }
}
