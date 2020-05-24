package com.lowi

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Data2Hbase {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setAppName("Data2Hbase").setMaster("local[2]")
    //2.构建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.读取数据文件
    val data: RDD[Array[String]] = sc.textFile("E:\\lowi\\data\\users.dat").map(_.split("::"))
    //4.保存结果数据到hbase表
    data.foreachPartition(ite => {
      //4.1获取hbase的数据库
      val configuration: Configuration = HBaseConfiguration.create()
      //指定zk集群的地址
      configuration.set("hbase.zookeeper.quorum","node1:2181,node2:2181,node3:2181")
      val connection: Connection = ConnectionFactory.createConnection(configuration)
      //4.2 对于hbase表进行操作。，这里需要一个Table对象
      val table: Table = connection.getTable(TableName.valueOf("person"))
      //4.3把数据保存早表中
      try{
          ite.foreach(x => {
            val put = new Put(x(0).getBytes())
            val puts = new util.ArrayList[Put]()
            //构建数据
            val put1: Put = put.addColumn("f1".getBytes,"gender".getBytes,x(1).getBytes())
            val put2 = put.addColumn("f1".getBytes,"age".getBytes,x(2).getBytes)
            val put3 = put.addColumn("f2".getBytes,"position".getBytes,x(3).getBytes())
            val put4: Put = put.addColumn("f2".getBytes(),"code".getBytes,x(4).getBytes)
            puts.add(put1)
            puts.add(put2)
            puts.add(put3)
            puts.add(put4)
            //提交数据
            table.put(puts)
          })
      }catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (connection != null) {
          connection.close()
        }
      }

    })
    sc.stop()
  }
}
