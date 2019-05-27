package com.hank.example.variable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * 共享变量高级特性：Broadcast广播变量(重点掌握)
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorVariable");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //2.x版本中已经弃用
//        final Accumulator<Integer> sum=sc.accumulator(0);

        LongAccumulator sums = sc.sc().longAccumulator("sum");

        List<Integer> numberList = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t){
                sums.add(1);
            }
        });

        // 在driver程序中，可以调用Accumulator的value()方法，获取其值??
        System.out.println(sums.value());
        sc.close();
    }
}
