package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
  * def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
  * def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]
  *
  * 该函数用于RDD[K,V]根据K将V做折叠、合并处理，
  * 其中的参数zeroValue表示先根据映射函数将zeroValue应用于V,进行初始化V,再将映射函数应用于初始化后的V.
  */
object FoldByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("FoldByKeyTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    rdd1.foldByKey(0)(_+_).collect().foreach(println)
    /**
      * 结果
      * Array[(String, Int)] = Array((A,2), (B,3), (C,1))
      * //将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,映射函数为+操
      * //作，比如("A",0), ("A",2)，先将映射函数应用于V，得到(A,0+2),即(A,2),
      * 再将zeroValue应用于每个key,得到：("A",0+2)
      */
    println("=================================")
    rdd1.foldByKey(5)(_+_).collect().foreach(println)
    /**
      * 结果
      * Array[(String, Int)] = Array((A,7), (B,8), (C,6))
      * //将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,映射函数为+操
      * //作，比如("A",0), ("A",2)，先将映射函数应用于V，得到(A,0+2),即(A,2),
      * 再将zeroValue应用于每个key,得到：("A",5+2)
      */
    println("=================================")
    rdd1.foldByKey(0)(_*_).collect().foreach(println)
    /**
      * 结果
      * Array[(String, Int)] = Array((A,0), (B,0), (C,0))
      * //将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,映射函数为*操
      * //作，比如("A",0), ("A",2)，先将映射函数应用于V，得到(A,0*2),即(A,0),
      * 再将zeroValue应用于每个key,得到：("A",0*2)
      */

    println("=================================")
    rdd1.foldByKey(3)(_*_).collect().foreach(println)
    /**
      * 结果
      * Array[(String, Int)] = Array((A,0), (B,6), (C,3))
      * //将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,映射函数为*操
      * //作，比如("B",1),("B",2)，先将映射函数应用于V，得到(B,1*2),即(B,2),
      * 再将zeroValue应用于每个key,得到：("B",3*2)
      */

    sc.stop()
  }
}
