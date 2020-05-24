package com.lowi.sparkSql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

//todo 通过sparksql把结果数据写入mysql表中
/**
  * 读取数据库中的数据，使用sparksql处理后
  * 再写入数据库中
  */
object Data2Mysql {
  def main(args: Array[String]): Unit = {
    //1.创建sparksession
    val spark: SparkSession = SparkSession.builder()
      .appName("Data2Mysql")
      .master("local[2]")
      .getOrCreate()

    //集群方式
//    val spark: SparkSession = SparkSession.builder()
//      .appName("Data2Mysql")
//      .getOrCreate()

    //2.读取mysql表中的数据
    //2.1 定义url连接
    val url = "jdbc:mysql://node1:3306/spark"
    val table = "person"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","!Qaz123456")

    val mysqlDF: DataFrame = spark.read.jdbc(url,table,properties)
    //把dataFrame注册成一张表
    mysqlDF.createTempView("person")

    //通过sparkSession 调用sql
    //将大于30岁记录，保存到mysql表中
    val result: DataFrame = spark.sql("select * from person where age > 30")
    //保存结果数据到mysql表中
    //mode:指定数据插入模式
    //overwrite:表示覆盖，如果表不存在，事先帮我们创建
    //append:表示追加，如果表不存在，事先帮我们创建
    //ignore:表示忽略，如果表事先存在，就不进行任何操作
    //error:如果表事先存在就报错(默认)
    result.write.mode("append").jdbc(url,"kperson",properties)

    //集群方式:args(0)插入模式；args(1)表名
//    result.write.mode(args(0)).jdbc(url,args(1),properties)

    //集群
    //关闭
    spark.stop()
  }
}
