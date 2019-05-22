package com.hank.example;

import com.hank.example.util.HbaseUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka计算api调用QPS并存入Hbase
 */

public class KafkaWordCount {
    public static void main(String[] args) throws InterruptedException{
        String host ="dev11.centos.68:9092";
        String topic = "xmiss";

        //创建JavaStreamingContext
        SparkConf conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext sc =new JavaStreamingContext(conf, Durations.seconds(1));

        //用Map设置参数
        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers",host);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "456");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topic);

        JavaInputDStream<ConsumerRecord<String,String>> stream =
                KafkaUtils.createDirectStream(
                        sc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(record -> new Tuple2<>(record.key(),record.value()));

        stream.foreachRDD(rdd->{

            rdd.foreachPartition(records ->{
                int count =0;
                while (records.hasNext()){
                    String line = records.next().value();
                    System.out.println(line);

                    if(line.contains("|serviceapi|")){
                        count++;
                    }
                }
                System.out.println("count value ====="+count);

                //保存至Hbase
                HbaseUtil.addQps(count);
            });
        });

        sc.start();
        sc.awaitTermination();
    }
}
