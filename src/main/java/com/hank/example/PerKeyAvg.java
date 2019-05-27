package com.hank.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 键值对操作combineByKey 求平均值 要重点理解
 */
public final class PerKeyAvg {
    public static class AvgCount implements java.io.Serializable{
        public int total_;
        public int num_;
        public AvgCount(int total,int num){
            total_ = total;
            num_ = num;
        }
        public float avg(){
            return total_ / (float) num_;
        }
    }

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("perKeyAvg");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> inputs= new ArrayList();

        inputs.add(new Tuple2("coffee",1));
        inputs.add(new Tuple2("coffee",2));
        inputs.add(new Tuple2("pandas",3));

        JavaPairRDD<String,Integer> rdd = sc.parallelizePairs(inputs);

        Function<Integer,AvgCount> createAcc = new Function<Integer,AvgCount>(){
            @Override
            public AvgCount call(Integer x) {
                return new AvgCount(x,1);
            }
        };

        Function2<AvgCount,Integer,AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Integer x){
                a.total_ += x;
                a.num_ += 1;
                return a;
            }
        };

        Function2<AvgCount,AvgCount,AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b){
                a.total_ += b.total_;
                a.num_ += b.num_;
                return a;
            }
        };

        AvgCount initial = new AvgCount(0,0);
        JavaPairRDD<String,AvgCount> avgCounts=rdd.combineByKey(createAcc,addAndCount,combine);
        Map<String,AvgCount> countMap =avgCounts.collectAsMap();

        for(Map.Entry<String,AvgCount> entry : countMap.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }
}
