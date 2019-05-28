package com.hank.example.top;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 分组取top3
 * top在面试中经常遇到要重点掌握
 */
public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GroupTop3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D://ideaproject//sparkstudy//files//score.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lineSplited = s.split(" ");
                return new Tuple2(lineSplited[0], Integer.valueOf(lineSplited[1]));
            }
        });

        //{"class1",90} {"class2":80} {"class1",78} {"class2",95}
        //{"class1",{90,78}} {"class2",{80,95}}
        JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Score = groupedPairs.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                    @Override
                    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classScores) throws Exception {

                        String className = classScores._1;
                        Iterator<Integer> scores = classScores._2.iterator();

                        Integer[] top3 = new Integer[3];

                        //遍历
                        while (scores.hasNext()){
                            Integer score = scores.next();

                            for(int i = 0; i < 3; i++) {
                                if(top3[i] == null) {
                                    top3[i] = score;
                                    break;
                                } else if(score > top3[i]) {
                                    for(int j = 2; j > i; j--) {
                                        top3[j] = top3[j - 1];
                                    }
                                    top3[i] = score;
                                    break;
                                }
                            }
                        }

                        return new Tuple2(className, Arrays.asList(top3));
                    }
                });

        top3Score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class："+t._1);

                Iterator<Integer> scoreIterator = t._2.iterator();

                while (scoreIterator.hasNext()){
                    Integer score =scoreIterator.next();
                    System.out.println(score);
                }
                System.out.println("=======================================");
            }
        });

        sc.close();
    }
}
