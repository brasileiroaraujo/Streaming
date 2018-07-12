package KafkaIntegration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


//Parallel-based Metablockig for Streaming Data
public class SaveKafkaResultsPRIME {
  public static void main(String[] args) throws InterruptedException {
	  String OUTPUT_PATH = "C:\\Users\\lutibr\\Documents\\outputs\\teste77\\";
	  SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
        .master("local[2]")
        .getOrCreate();
    
//    spark.sparkContext().getConf().set("spark.driver.memory", "4g");
    
    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    

    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    
    
    //Notice that Spark Streaming is not designed for periods shorter than about half a second. If you need a shorter delay in your processing, try Flink or Storm instead.
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(12000));
    //checkpointing is necessary since states are used
    ssc.checkpoint("C:\\Users\\lutibr\\Documents\\checkpoint\\");

    //kafka pool to receive streaming data
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", "localhost:9092");
    Set<String> topics = Collections.singleton("outputTopic");

    JavaPairInputDStream<String, String> streamOfRecords = KafkaUtils.createDirectStream(ssc,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
    
    
    JavaPairDStream<Integer, List<String>> streamOfItems = streamOfRecords.mapToPair(new PairFunction<Tuple2<String,String>, Integer, List<String>>() {

		@Override
		public Tuple2<Integer, List<String>> call(Tuple2<String, String> s) throws Exception {
			return new Tuple2(Integer.parseInt(s._2().split(",")[0]), Arrays.asList(s._2().split(",")));
		}
	});
    
    
	 // save all results (including all pre calculated)
	Function2<List<List<String>>, Optional<List<String>>, Optional<List<String>>> updateOutputFunction = new Function2<List<List<String>>, Optional<List<String>>, Optional<List<String>>>() {
		@Override
		public Optional<List<String>> call(List<List<String>> values, Optional<List<String>> state)
				throws Exception {
			List<String> storedNode = state.or(new ArrayList<String>());
			for (List<String> newValues : values) {
//				newValues.remove(0);//the source entity
				storedNode.addAll(newValues);
			}
			
			return Optional.of(storedNode);
		}
	};

 		// save the output in state
 	JavaPairDStream<Integer, List<String>> PRIMEoutput = streamOfItems.updateStateByKey(updateOutputFunction);
    
	
 	PRIMEoutput.foreachRDD(rdd ->{
		System.out.println("Batch size: " + rdd.count());
		
			
	    if(!rdd.isEmpty()){
	       rdd.saveAsTextFile(OUTPUT_PATH);
	    }
	});
    
    
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();
    ssc.awaitTermination();
    System.out.println("*** Streaming terminated");
  }
}
