package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 转化操作实战
 */
public class TransformationOperation {
    public static void main(String[] args) {
//        map();
//        filter();
//        flatmap();
//        groupByKey();
        reduceByKey();
    }

    /**
     * map转化:每个元素乘以2
     */
    private static void map(){
         SparkConf conf = new SparkConf().
                 setMaster("local").setAppName("map");

         //JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        JavaRDD<Integer> multipleNumberRDD=numberRDD.map(new Function<Integer, Integer>() {
               @Override
               public Integer call(Integer v1){
                   return v1 * 2;
                }
        });

        multipleNumberRDD.foreach(new VoidFunction<Integer>(){
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });
        //关闭JavaSparkContext
        sc.close();
    }

    /**
     * filter转化:过滤集合中的偶数
     */
    private static void filter(){
        SparkConf conf = new SparkConf().
                setAppName("filter").setMaster("local");

        //JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRdd = sc.parallelize(numbers);

        //filter返回类型事Boolean,返回true则保留元素，返回false,则不想保留该元素
        JavaRDD<Integer> answer = numberRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer%2 != 0;
            }
        });

        answer.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        //关闭JavaSparkContext
        sc.close();
    }

    /**
     * flatMap转化:将文本行拆分为多个单词
     */
    private static void flatmap(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("flatmap");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lineList = Arrays.asList("Hello you","hello me","hello word");

        JavaRDD<String> lines = sc.parallelize(lineList);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        //关闭JavaSparkContext
        sc.close();
    }

    /**
     * 键值对groupByKey，按照班级对成绩进行排序
     */
    public static void groupByKey(){
        //创建SpackConf
        SparkConf conf = new SparkConf().setMaster("local").setAppName("groupByKey");

        //JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //模拟集合创建键值对
        List<Tuple2<String,Integer>> sorceList = Arrays.asList(
                new Tuple2<String,Integer>("class1",80),
                new Tuple2<String,Integer>("class2",75),
                new Tuple2<String,Integer>("class1",90),
                new Tuple2<String,Integer>("class2",65)
        );

        JavaPairRDD<String,Integer> scores =sc.parallelizePairs(sorceList);

        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: "+t._1());

                Iterator<Integer> ite = t._2().iterator();
                while(ite.hasNext()){
                    System.out.println(ite.next());
                }
                System.out.println("==============================");
            }
        });
        //关闭JavaSparkContext
        sc.close();
    }

    /**
     * 键值对reduceByKey:统计每个班级的总分
     */
    public static void reduceByKey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduceByKey");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
            new Tuple2<String,Integer>("class1",80),
                new Tuple2<String,Integer>("class2",75),
                new Tuple2<String,Integer>("class1",90),
                new Tuple2<String,Integer>("class2",65)
        );

        JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);

        //键值对操作
        JavaPairRDD<String,Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer score1, Integer score2) throws Exception {
                return score1 + score2;
            }
        });

        //foreach
        totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
    }
}
