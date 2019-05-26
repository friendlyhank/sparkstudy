package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ActionOperation {
    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();

    }

    /**
     * map是转换操作，对每个元素进行操作返回新的元素
     * reduce是行动操作，讲元素聚合求值
     */
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

    private static void collect(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("collect");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });
        
        List<Integer> doubleNumberList = doubleNumbers.collect();

       for(Integer num : doubleNumberList){
           System.out.println(num);
       }

       sc.close();
    }

    private static void count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("count");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,6,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        long sums = numbers.count();

        System.out.println(sums);

        sc.close();
    }

    private static void take(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("take");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        List<Integer> tokenNums = numbers.take(4);

        for(Integer num : tokenNums){
            System.out.println(num);
        }
    }
}
