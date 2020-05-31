package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)]
  * def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)]
  * def combineByKey[C](createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean = true, serializer: Serializer = null): RDD[(K, C)]
  * 该函数用于将RDD[K,V]转换成RDD[K,C],这里的V类型和C类型可以相同也可以不同。
  * 其中的参数：
  * createCombiner：组合器函数，用于将V类型转换成C类型，输入参数为RDD[K,V]中的V,输出为C ,分区内相同的key做一次
  * mergeValue：合并值函数，将一个C类型和一个V类型值合并成一个C类型，输入参数为(C,V)，输出为C，分区内相同的key循环做
  * mergeCombiners：分区合并组合器函数，用于将两个C类型值合并成一个C类型，输入参数为(C,C)，输出为C，分区之间循环做
  * numPartitions：结果RDD分区数，默认保持原有的分区数
  * partitioner：分区函数,默认为HashPartitioner
  * mapSideCombine：是否需要在Map端进行combine操作，类似于MapReduce中的combine，默认为true
  */
object CombineByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("combineByKey").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("C",1)), 3)

    /**
      * 其中三个映射函数分别为：
      * createCombiner: (V) => C
      * (v : Int) => v + “_” //在每一个V值后面加上字符_，返回C类型(String)
      * mergeValue: (C, V) => C
      * (c : String, v : Int) => c + “@” + v //合并C类型和V类型，中间加字符@,返回C(String)
      * mergeCombiners: (C, C) => C
      * (c1 : String, c2 : String) => c1 + “$” + c2 //合并C类型和C类型，中间加$，返回C(String)
      * 其他参数为默认值。
      * 最终，将RDD[String,Int]转换为RDD[String,String]。
      */
    rdd1.combineByKey(
      (v:Int) => v + "_",
      (c:String, v:Int) => c+"@"+v,
      (c1:String, c2:String) => c1 +"$"+c2
    ).collect().foreach(println)

    /**
      * 结果
      * Array[(String, String)] = Array((A,2_$1_), (B,1_$2_), (C,1_))
      */

    println("=======================================")

    rdd1.combineByKey(
      (v:Int)=>List(v),
      (c:List[Int], v:Int) =>v::c,
      (c1:List[Int], c2:List[Int]) => c1:::c2
    ).collect().foreach(println)

    /**
      * 结果
      * Array[(String, List[Int])] = Array((A,List(2, 1)), (B,List(2, 1)), (C,List(1)))
      * 最终将RDD[String,Int]转换为RDD[String,List[Int]]。
      */

    sc.stop()
  }

}
