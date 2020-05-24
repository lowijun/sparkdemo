package com.lowi.test20200523

import org.apache.spark.SparkConf

/**
  * 统计:
  * 1.数据中所有人的平均年龄
  * 2.数据中所有男性未婚的人数和女性未婚人数
  * 3.数据中20-30已婚数量前3的省份
  * 4.未婚比例(未婚人数/该城市总人数)最高的前3个城市
  */
object UserInfo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
  }

}
