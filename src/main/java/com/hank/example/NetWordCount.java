package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Socket连接案例计算WordCount
 */

public class NetWordCount {
    public static void main(String[] args)throws InterruptedException {
        //创建JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> netTextFile = sc.socketTextStream("localhost",9999);

        JavaPairDStream<String,Integer> wordCountResult = netTextFile
                .flatMap(line-> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .reduceByKey((a,b) -> a+b);

        //4. 保存结果 foreach(println) 【Actions】
        wordCountResult.print();

        sc.start();
        sc.awaitTermination();
    }
}
