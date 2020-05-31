package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * def reduceByKeyLocally(func: (V, V) => V): Map[K, V]
  *
  * 该函数将RDD[K,V]中每个K对应的V值根据映射函数来运算，运算结果映射到一个Map[K,V]中，而不是RDD[K,V]。
  */
object ReduceByKeyLocallyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ReduceByKeyLocallyTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    val rdd2: collection.Map[String, Int] = rdd1.reduceByKeyLocally((x, y)=>x+y)

    rdd2.foreach(println)
    /**
      * 结果
      * scala.collection.Map[String,Int] = Map(B -> 3, A -> 2, C -> 1)
      */
    sc.stop()
  }
}
