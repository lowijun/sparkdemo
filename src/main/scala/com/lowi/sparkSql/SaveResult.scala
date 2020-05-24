package com.lowi.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo  sparksql可以把结果数据保存到不同的外部存储介质中
object SaveResult {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SaveResult")
    //2.创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.加载数据
    val jsonDF: DataFrame = spark.read.json("E:\\lowi\\data\\score.json")
    //4.把DataFrame注册成表
    jsonDF.createTempView("t_score")
    jsonDF.printSchema()
    jsonDF.show()

    //5.统计分析
    val result: DataFrame = spark.sql("select * from t_score where score>80")
    //保存结果数据到不同的外部存储介质中
    //5.1保存结果数据到文本文件----保存数据成文本文件目录只支持单个字段，不支持多个字段
    result.select("lastName").write.text("./data/result/123.txt")

    //5.2保存结果数据到json文件
    result.write.json("./data/json")

    //5.3保存数据到parquet文件
    result.write.parquet("./data/parquet")

    //5.4save方法保存结果数据，默认的数据格式就是parquet
    result.write.save("./data/save")

    //5.5 保存结果数据到csv文件
    result.write.csv("./data/csv")

    //5.6 保存结果数据到数据表中
    result.write.saveAsTable("t1")

    //5.7 按照单个字段进行分区 分目录进行存储
    result.write.partitionBy("lastName").json("./data/partitions")

    //5.8 按照多个字段进行分区 分目录进行存储
    result.write.partitionBy("lastName","firstName").json("./data/numPartitions")

    spark.stop()
  }
}
