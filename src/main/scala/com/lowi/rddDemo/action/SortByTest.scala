package com.lowi.rddDemo.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.LongAccumulator

/**
  * def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
  *
  * sortBy根据给定的排序k函数将RDD中的元素进行排序。
  */
object SortByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SortByTest")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[Int] = sc.makeRDD(Seq(3,6,7,1,2,0),2)
    rdd1.sortBy(x=>x).collect()
    /**
      * 结果
      * Array[Int] = Array(0, 1, 2, 3, 6, 7) //默认升序
      */

    rdd1.sortBy(x=>x, false).collect()
    /**
      * 结果
      * Array[Int] = Array(7, 6, 3, 2, 1, 0)  //降序
      */
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
    rdd2.sortBy(x=>x).collect()
    /**
      * 结果
      * Array[(String, Int)] = Array((A,1), (A,2), (B,3), (B,6), (B,7))
      */
    rdd2.sortBy(x=>x._2, false).collect()
    /**
      * 结果
      * 按照V进行降序排序
      * Array[(String, Int)] = Array((B,7), (B,6), (B,3), (A,2), (A,1))
      */
    sc.stop()
  }
}
