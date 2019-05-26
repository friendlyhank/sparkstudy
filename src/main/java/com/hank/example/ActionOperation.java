package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionOperation {
    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();
//        saveAsTextFile();
        countByKey();
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

    /**
     * collect行动操作: 不要再集群中使用，消耗大量资源把元素集合到一台机子上
     */
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

    /**
     * count行动操作， 元素的个数
     */
    private static void count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("count");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,6,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        long sums = numbers.count();

        System.out.println(sums);

        sc.close();
    }

    /**
     * take行动操作 获取前4个元素
     */
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

    /**
     * saveAsTextFile行动操作  将结果保存在hdfs(这个要掌握)
     * 在window会报错，因为不是root用户没有权限操作
     */
    private static void saveAsTextFile(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("saveAsTextFile");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer *2 ;
            }
        });

        doubleNumbers.saveAsTextFile("hdfs://spark1:8020/spark/double_number.txt");

        sc.close();
    }

    /**
     * 键值对行动操作：每个键对应的元素计数
     */
    private static void countByKey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("countByKey");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,String>> scoreList =Arrays.asList(
                new Tuple2<String,String>("class1","leo"),
                new Tuple2<String,String>("class2","jack"),
                new Tuple2<String,String>("class1","marry"),
                new Tuple2<String,String>("class2","tom"),
                new Tuple2<String,String>("class2","david")
        );

        JavaPairRDD<String,String> students= sc.parallelizePairs(scoreList);

        Map<String,Long> studentCounts = students.countByKey();

        for(Map.Entry<String,Long> studentCount : studentCounts.entrySet()){
            System.out.println(studentCount.getKey()+"："+studentCount.getValue());
        }
    }
}
