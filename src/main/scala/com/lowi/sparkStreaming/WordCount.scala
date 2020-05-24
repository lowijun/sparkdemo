package com.lowi.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //步骤一：初始化程序入口
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")
    //步骤二：获取数据流
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //步骤三：数据处理
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word,1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    //步骤四：数据输出
    wordCounts.print()
    //步骤五：启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
