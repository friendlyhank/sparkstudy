package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * spark集群中运行
 */

public class WordCountCluster {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        //连接集群可以把setmaster去掉，自动会连接
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal");

        //创建JavaSparkContext对象
        //在spark中,JavaSparkContext是Spark所有功能的入口
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("hdfs://spark1:8020/spark/spark.txt");

        //(2)
//        if (args.length < 1){
//            System.out.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }

        //Spark-SQL包 SparkSession
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaWordCount")
//                .getOrCreate();
//
//        //readfile 读取文件
//        JavaRDD<String> lines= spark.read().textFile(args[0]).javaRDD();

        //faltmap 每个元素进行分割符分割,每一行分割为单词
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        //maptopair 转换成键值对，映射成(word,1) word为key，1为值，后面就可以进行统计操作了
        JavaPairRDD<String, Integer> ones = words.mapToPair(s->new Tuple2<>(s,1));

        //reducebykeys //键值对合并操作，相同的键值对统计相加
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2)->i1+i2);

        //输出 collect要慎用在集群中，会把数据集中在一台机器
        List<Tuple2<String, Integer>> output =counts.collect();
        for(Tuple2<?,?> tuple : output){
            System.out.println(tuple._1()+"："+tuple._2());
        }
    }
}
