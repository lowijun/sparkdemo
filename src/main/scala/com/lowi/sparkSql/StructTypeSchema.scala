package com.lowi.sparkSql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo 通过动态指定dataFrame对应的schema信息将rdd转换成dataFrame
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    //1.构建sparkSession
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //获取sparkContext
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    //3.读取文件数据
    val data: RDD[Array[String]] = sc.textFile("E:\\lowi\\data\\person.txt").map(x => x.split(","))
    //将rdd与row进行关联
    val rowRDD: RDD[Row] = data.map(x => Row(x(0),x(1),x(2).toInt))
    //指定dataFrame的schema信息
    //这里指定的字段个数和类型要跟Row保持一致
    val schema: StructType = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil)

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)
    dataFrame.printSchema()
    dataFrame.show()

    dataFrame.createTempView("user")
    spark.sql("select * from user ").show()

    spark.stop()
  }

}
