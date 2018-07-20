package KafkaIntegration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;


public class KafkaConsumer {
	
	public static void main(String[] args) throws InterruptedException {
//    	System.setProperty("hadoop.home.dir", "K:\\winutils");
    	
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "153.1.65.46:9092");
        Set<String> topics = Collections.singleton("topicTest");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        
//        directKafkaStream.dstream().saveAsTextFiles("outputs/stream", "");
        
        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println(record._2()));
//            rdd.saveAsTextFile("outputs/teste1/");
        });
        
        ssc.start();
        ssc.awaitTermination();
    }
//	public static void main(String[] args) {
//		final Consumer<Long, String> consumer = createConsumer();
//        final int giveUp = 100;   
//        int noRecordsCount = 0;
//        while (true) {
//            final ConsumerRecords<Long, String> consumerRecords =
//                    consumer.poll(1000);
//            if (consumerRecords.count()==0) {
//                noRecordsCount++;
//                if (noRecordsCount > giveUp) break;
//                else continue;
//            }
//            consumerRecords.forEach(record -> {
//                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                        record.key(), record.value(),
//                        record.partition(), record.offset());
//            });
//            consumer.commitAsync();
//        }
//        consumer.close();
//        System.out.println("DONE");
//	}
//	
//	private static Consumer<Long, String> createConsumer() {
//	      final Properties props = new Properties();
//	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
//	    		  "153.1.65.46:9092");
//	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
//	                                  "KafkaExampleConsumer");
//	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//	              LongDeserializer.class.getName());
//	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//	              StringDeserializer.class.getName());
//	      // Create the consumer using props.
//	      final Consumer<Long, String> consumer =
//	                                  new org.apache.kafka.clients.consumer.KafkaConsumer(props);
//	      // Subscribe to the topic.
//	      consumer.subscribe(Collections.singletonList("topicTest"));
//	      return consumer;
//	  }
	
	
}
