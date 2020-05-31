package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * def partitionBy(partitioner: Partitioner): RDD[(K, V)]
  * 该函数根据partitioner函数生成新的ShuffleRDD，将原RDD重新分区。
  * scala> var rdd1 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
  * rdd1: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[23] at makeRDD at :21
  * scala> rdd1.partitions.size
  * res20: Int = 2
  */
object PartitionByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[2]").setAppName("PartitionByTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1,"A"), (2, "B"), (3, "C"), (4, "D")), 2)
    //查看每个分区的元素
    rdd1.mapPartitionsWithIndex {
      (idx, iter) => {
        var part_map: mutable.Map[String, List[(Int, String)]] = scala.collection.mutable.Map[String, List[(Int, String)]]()
        while (iter.hasNext) {
          var part_name: String = "part_" + idx
          var elem: (Int, String) = iter.next()
          if (part_map.contains(part_name)) {
            var elems: List[(Int, String)] = part_map(part_name)
            //            elems.::=elem
            elems ::= elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int, String)]{elem}
          }
        }
        part_map.iterator
      }
    }.collect().foreach(println)

    /**
      * 结果
      * Array[(String, List[(Int, String)])] = Array((part_0,List((2,B), (1,A))), (part_1,List((4,D), (3,C))))
      * //(2,B),(1,A)在part_0中，(4,D),(3,C)在part_1中
      */

    println("===============================================")
    //使用partitonby进行重分区
    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
    rdd2.mapPartitionsWithIndex{
      (idx, iter) =>{
        var part_map: mutable.Map[String, List[(Int, String)]] = scala.collection.mutable.Map[String, List[(Int, String)]]()
        while(iter.hasNext) {
          var part_name: String = "part_" + idx
          var elem: (Int, String) = iter.next()
          if(part_map.contains(part_name)) {
            var elems: List[(Int, String)] = part_map(part_name)
            elems::=elem
            part_map(part_name) = elems
          } else {
            part_map(part_name) = List[(Int, String)]{elem}
          }
        }
        part_map.iterator
      }
    }.collect().foreach(println)

    /**
      * 结果
      * Array[(String, List[(Int, String)])] = Array((part_0,List((4,D), (2,B))), (part_1,List((3,C), (1,A))))
      * //(4,D),(2,B)在part_0中，(3,C),(1,A)在part_1中
      */

    sc.stop()
  }
}
