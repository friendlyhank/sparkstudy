package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Persist持久化的案例
 * 结果:
 * 22562
 * cost 586 milliseconds.
 *
 * 22562
 * cost 37 milliseconds.
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("persist");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D://ideaproject//sparkstudy//files//spark.txt")
                .cache();

        long beginTime=  System.currentTimeMillis();

        //第一次运行count行动操作先不会读取缓存，而是写入缓存
        long counts = lines.count();
        System.out.println(counts);

        long endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds.");

        beginTime = System.currentTimeMillis();

        counts= lines.count();
        System.out.println(counts);

        endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds.");

        sc.close();
    }
}
