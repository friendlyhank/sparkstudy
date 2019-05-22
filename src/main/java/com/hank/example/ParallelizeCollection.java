package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 *  * 案例：累加1到10
 *  集合主要用于测试
 */

public class ParallelizeCollection {
    public static void main(String[] args) {
       //创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //action 相加操作reducebyKey是集合的相加操作
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>(){
            @Override
            public Integer call(Integer num1, Integer num2){
                return num1+num2;
            }
        });

        //
        System.out.println("1到10的累加和："+sum);

        //关闭JavaSparkContext
        sc.close();
    }
}
