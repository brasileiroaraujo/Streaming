package KafkaIntegration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import DataStructures.EntityProfile;
import kafka.serializer.StringDecoder;

public class SparkStreamingConsumerKafka {

    public static void main(String[] args) throws InterruptedException {
//    	System.setProperty("hadoop.home.dir", "K:\\winutils");
    	
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("mytopic");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        
//        directKafkaStream.dstream().saveAsTextFiles("outputs/stream", "");
        
        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
//            rdd.foreach(record -> System.out.println(new EntityProfile(record._2()).isSource()));
            rdd.saveAsTextFile("outputs/teste1/");
        });
        
        ssc.start();
        ssc.awaitTermination();
    }
}