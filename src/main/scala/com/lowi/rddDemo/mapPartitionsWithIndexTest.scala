package com.lowi.rddDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitionsWithIndexTest {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd1: RDD[Int] = sc.makeRDD(1 to 5, 2)

    /**
      * def mapPartitionsWithIndex[U]
      *   (f: (Int, Iterator[T]) => Iterator[U],
      *   preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
      *
      *   mpwi将rdd1中每个分区的数字累加，并在每个分区的累加结果前面加了分区索引
      */
    val mpwi: RDD[Any] = rdd1.mapPartitionsWithIndex {
      (x, iter) => {
        var result: List[Any] = List[String]()
        var i = 0
        while (iter.hasNext) {
          i += iter.next()
        }
        result.::(x + "|" + i).iterator
      }
    }
    mpwi.collect().foreach(println)

    sc.stop()
  }
}
