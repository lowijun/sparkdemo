package com.lowi.test20200523

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkcore实现
  * 统计:
  * 1.数据中所有人的平均年龄
  * 2.数据中所有男性未婚的人数和女性未婚人数
  * 3.数据中20-30已婚数量前3的省份
  * 4.未婚比例(未婚人数/该城市总人数)最高的前3个城市
  *
  * 数据形式
  * 张吉惟|男|23|未婚|北京|海淀
  * 林国瑞|男|25|已婚|河北|石家庄
  * 林玟书|男|29|未婚|北京|朝阳
  */
object UserInfo {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("userInfo").setMaster("local[2]")
    //创建sparkcontext
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //获取数据
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\userinfo.txt")
    //数据处理
    val structRDD: RDD[(String, String, Int, String, String, String)] = data.map(t => {
      val strs: Array[String] = t.split("\\|")
      val name: String = strs(0)
      val sex: String = strs(1)
      val age: Int = strs(2).toInt
      val isMarry: String = strs(3)
      val province: String = strs(4)
      val city: String = strs(5)
      (name, sex, age, isMarry, province, city)
    })
    //数据缓存，后面多次被使用
    structRDD.cache()

    //1.数据中所有人的平均年龄  张吉惟|男|23|未婚|北京|海淀
    val accumulator: LongAccumulator = sc.longAccumulator
    val ageCount: Int = structRDD.map(t => {
      accumulator.add(1)
      t._3
    }).reduce(_ + _)
    ageCount
    val ageNumber: lang.Long = accumulator.value
    val avgAge: Double = ageCount.toLong / (ageNumber * 1.0)
    println(s"所有人平均年龄为${avgAge}")

    println("2.数据中所有男性未婚的人数和女性未婚人数")
    //2.数据中所有男性未婚的人数和女性未婚人数
    val isMarrALL: RDD[(String, Iterable[(String, String)])] = structRDD.map(t => {
      (t._2, t._4)
    }).filter(_._2.equals("未婚")).groupBy(_._1)  //按照性别分组

    val res2Map: RDD[(String, Int)] = isMarrALL.mapValues(t => t.size)
    res2Map.collect().foreach(println)

    println("3.数据中20-30岁已婚数量前3的省份")
    //3.数据中20-30岁已婚数量前3的省份  张吉惟|男|23|未婚|北京|海淀
    val res3RDD: Array[(Int, String)] = structRDD.filter(t => {
      t._3 >= 20 && t._3 <= 30 && t._4.equals("已婚") //过滤20-30岁所有已婚数据
    }).groupBy(_._5)
      .mapValues(_.size)
      //k v互换，排序，取前三
      .map(t => (t._2, t._1))
      .top(3)
    res3RDD.foreach(println)

    println("4.未婚比例(未婚人数/该城市总人数)最高的前3个城市")
    //4.未婚比例(未婚人数/该城市总人数)最高的前3个城市
    val result1: RDD[(String, Int)] = structRDD.map(t => {
      (t._5, t._4)
    }).filter(_._2.equals("未婚"))
      .groupBy(_._1)   //按省份分组
      .map(t => {
        (t._1, t._2.size)
      })
    result1.foreach(println)

    println("====总数====")
    //总数
    val result2: RDD[(String, Int)] = structRDD.map(t => (t._5, 1)).reduceByKey(_+_)
    result2.foreach(println)

    println("====整合====")
    //整合
    val result3: RDD[(String, (Int, Int))] = result1.join(result2)
    result3.foreach(println)
    println("====最终结果====")
    result3.map(t => {(t._1, t._2._1.toDouble/ t._2._2.toDouble)}).sortBy(_._2, false).take(3).foreach(println)

    //关闭
    sc.stop()
  }

}
