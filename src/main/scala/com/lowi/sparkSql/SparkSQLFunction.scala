package com.lowi.sparkSql

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo 自定义sparksql的UDF函数  一对一关系
object SparkSQLFunction {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("SparkSQLFunction")
      .master("local[2]")
      .getOrCreate()
    //2.构建数据源生产DataFrame
    val dataFrame: DataFrame = sparkSession.read.text("E:\\lowi\\data\\person.txt")
    //3.注册成表
    dataFrame.createTempView("person")

    //4.实现自定义的UDF函数
    //小写转大写
    sparkSession.udf.register("low2Up",new UDF1[String,String] {
      override def call(t1: String): String = {
        t1.toUpperCase
      }
    },StringType)

    //大写转小写
    sparkSession.udf.register("up2low",(x:String) => x.toLowerCase())

    //4.把数据文件中的单词同意转换成大小写
    sparkSession.sql("select value from person").show()
    sparkSession.sql("select low2Up(value) from person").show()
    sparkSession.sql("select up2low(value) from person").show()

    sparkSession.stop()

  }
}
