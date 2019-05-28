package com.hank.example.top;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 去最大的前三个数字
 * top在面试中经常遇到要重点掌握
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Top3");

        JavaSparkContext sc =new JavaSparkContext(conf);

        //注意一定是String类型
        JavaRDD<String> lines= sc.textFile("D://ideaproject//sparkstudy//files//top.txt");

        //键值对转化 sortByKey,所以要把数字转化为键值
        JavaPairRDD<Integer,String> pair = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2(Integer.valueOf(s),s);
            }
        });

        JavaPairRDD<Integer,String> sortPair = pair.sortByKey(false);
        
        //重点理解JavaPairRdd还能转化回JavaRDD
        JavaRDD<Integer> sortedNumbers = sortPair.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> t){
                return t._1;
            }
        });

        List<Integer> sortedNumberList= sortedNumbers.take(4);

        for(Integer num : sortedNumberList){
            System.out.println(num);
        }
    }
}
