package KafkaIntegration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.MapAccumulator;
import DataStructures.Node;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import streaming.util.CSVFileStreamGeneratorER;
import streaming.util.CSVFileStreamGeneratorPMSD;
import streaming.util.JavaDroppedWordsCounter;
import streaming.util.JavaWordBlacklist;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
public class SaveKafkaResultsPRIME {
  public static void main(String[] args) throws InterruptedException {
	  String OUTPUT_PATH = "outputs/abtbyStr2/";
	  SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
        .master("local[1]")
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
    ssc.checkpoint("checkpoints/");

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
