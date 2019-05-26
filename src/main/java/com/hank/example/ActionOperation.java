package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ActionOperation {
    public static void main(String[] args) {
        reduce();
    }

    private static void reduce(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1+num2;
            }
        });

        System.out.println(sum);

        //关闭JavaSparkContext
        sc.close();
    }
}
