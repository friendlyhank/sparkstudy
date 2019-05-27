package com.hank.example.variable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * 共享变量高级特性：Broadcast广播变量(重点掌握)
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BroadcastVariable");

        JavaSparkContext sc =new JavaSparkContext(conf);

        final int factor =3;
        Broadcast<Integer> factorBroadcast= sc.broadcast(factor);

        List<Integer> numberlist = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> numbers = sc.parallelize(numberlist);

        JavaRDD<Integer> multipleNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                int factor = factorBroadcast.value();
                return v1 * factor;
            }
        });

        multipleNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();
    }
}
