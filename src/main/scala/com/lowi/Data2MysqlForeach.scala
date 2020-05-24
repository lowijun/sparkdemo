package com.lowi

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Data2MysqlForeach {
  def main(args: Array[String]): Unit = {
    //1.构建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Data2MysqlForeach").setMaster("local[2]")
    //构建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.读取数据文件
    val data: RDD[String] = sc.textFile("E:\\lowi\\data\\person.txt")
    //4.切分欸一行  //id name age
    val personRDD: RDD[(String, String, Int)] = data.map(_.split(",")).map(x=> (x(0),x(1),x(2).toInt))
    //5.把数据保存到mysql表中
    personRDD.foreach(line => {
      //每一条数据与mysql建立连接，把数据插入到mysql表操作
      //1.获取连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://node1:3306/spark","root","!Qaz123456")
      //2.定义插入数据的sql
      val sql = "insert into person(id,name,age) values (?,?,?)"
      //3.获取preparedStatement
      try{
        val ps: PreparedStatement = connection.prepareStatement(sql)
        //4.获取数据，给？赋值
        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setInt(3, line._3)

        ps.execute()
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if (connection != null) {
          connection.close()
        }
      }

    })
    sc.stop()
  }
}
