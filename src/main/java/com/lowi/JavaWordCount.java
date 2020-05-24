package com.lowi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//todo  利用java语言开发spark单词统计程序
public class JavaWordCount {
    public static void main(String[] args) {
        //创建sparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");

        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.3");
        //构建JavaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //读取数据文件
        JavaRDD<String> data = jsc.textFile("E:\\wordCount.txt");
        //切分每一行单词的所有单词  scala: data.flatMap(x => x.split(" "))
        JavaRDD<String> wordsJavaRDD = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return Arrays.asList(words).iterator();
            }
        });
        //每个单词记为1  scala: wordsJavaRDD.map(x => (x,1))
        JavaPairRDD<String, Integer> wordOne = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        //相同单词出现的次数累加  scala:wordOne.reduceByKey((x,y) => x+y)
        JavaPairRDD<String, Integer> result = wordOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //todo java语言，排序方式，只能按照key进行排序，所以需要将次数置为key,作为排序的关键
        //按照单词的次数,进行反转
        JavaPairRDD<Integer, String> reverseRDD = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });
        //降序排序（单词，次数）--> (次数，单词).sortByKey --> (单词，次数)
        JavaPairRDD<String, Integer> sortRDD = reverseRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2,t._1);
            }
        });
        //收集打印
        List<Tuple2<String, Integer>> finalResult =  sortRDD.collect();

        for (Tuple2<String, Integer> t:finalResult){
            System.out.println("单词：" + t._1 + "\t次数：" + t._2);
        }
        //关闭
        jsc.stop();
    }
}
