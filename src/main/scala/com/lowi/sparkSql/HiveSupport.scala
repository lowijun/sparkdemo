package com.lowi.sparkSql

import org.apache.spark.sql.SparkSession

//todo 利用sparksql操作hivesql
object HiveSupport {
  def main(args: Array[String]): Unit = {
    //1.构建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive支持
      .getOrCreate()
    //2.直接使用sparksession去操作hivesqk语句
    //2.1 创建一张hive表
    spark.sql("create table people (id string,name string,age int) row format delimited fields terminated by ','")
    //2.2 加载数据到hive表中
    spark.sql("load data local inpath './data/person.txt' into table people")
    //2.3 查询
    spark.sql("select * from people").show()

    spark.stop()
  }

}
