package com.lowi.sparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo 利用sparksql加载mysql表中的数据
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DataFromMysql").setMaster("local[2]")
    //2.创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //读取mysql表的数据
    var url = "jdbc:mysql://node1:3306/spark"
    var tablename = "person"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","!Qaz123456")

    val mysqlDF: DataFrame = spark.read.jdbc(url,tablename,properties)
    //打印schema信息
    mysqlDF.printSchema()
    //展示数据
    mysqlDF.show()

    //把dataFrame注册成表
    mysqlDF.createTempView("person")

    spark.sql("select * from person where age > 30").show()
    spark.stop()
  }
}
