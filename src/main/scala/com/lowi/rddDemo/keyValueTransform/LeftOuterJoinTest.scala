package com.lowi.rddDemo.keyValueTransform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
  * def leftOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))]
  * def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))]
  *
  * leftOuterJoin类似于SQL中的左外关联left outer join，返回结果以前面的RDD为主，关联不上的记录为空。只能用于两个RDD之间的关联，如果要多个RDD关联，多关联几次即可。
  * 参数numPartitions用于指定结果的分区数
  * 参数partitioner用于指定分区函数
  */
object LeftOuterJoinTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("LeftOuterJoinTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)

    val rdd3: RDD[(String, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
    rdd3.foreach(println)
    /**
      * 结果
      * (B,(2,None))
      * (A,(1,Some(a)))
      * (C,(3,Some(c)))
      */
    sc.stop()
  }
}
