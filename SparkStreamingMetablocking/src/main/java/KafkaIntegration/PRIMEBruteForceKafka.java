package KafkaIntegration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import kafka.serializer.StringDecoder;
import scala.Tuple2;


//Parallel-based Metablockig for Streaming Data
public class PRIMEBruteForceKafka {
  public static void main(String[] args) throws InterruptedException {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
        .master("local[1]")
        .getOrCreate();
    

    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    

    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    
    //We have configured the period to 8 seconds (8000 ms). 
    //Notice that Spark Streaming is not designed for periods shorter than about half a second. If you need a shorter delay in your processing, try Flink or Storm instead.
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(8000));
    
    //checkpointing is necessary since states are used
    ssc.checkpoint("checkpoints/");
    

    //kafka pool to receive streaming data 
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", "localhost:9092");
    Set<String> topics = Collections.singleton("mytopic");

    JavaPairInputDStream<String, String> streamOfRecords = KafkaUtils.createDirectStream(ssc,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<EntityProfile> streamOfItems = streamOfRecords.map(s -> new EntityProfile(s._2()));
    
    //Reading from dataset (streaming)
    JavaPairDStream<String, EntityProfile> streamOfPairs =
        streamOfItems.flatMapToPair(new PairFlatMapFunction<EntityProfile, String, EntityProfile>() {
			@Override
			public Iterator<Tuple2<String, EntityProfile>> call(EntityProfile se) throws Exception {
				Set<Tuple2<String, EntityProfile>> output = new HashSet<Tuple2<String, EntityProfile>>();
				
				Set<String> cleanTokens = new HashSet<String>();
				
				for (Attribute att : se.getAttributes()) {
					String[] tokens = att.getValue().split(" ");
					Collections.addAll(cleanTokens, tokens);
				}
				
				for (String tk : cleanTokens) {
					output.add(new Tuple2<>(tk, se));
				}
				
				return output.iterator();
			}
		});
    
    JavaPairDStream<String, Iterable<EntityProfile>> streamGrouped = streamOfPairs.groupByKey();

    
    //START THE METABLOCKING
    //coloca as tuplas no formato <e1, b1>
    JavaPairDStream<String, String> pairEntityBlock = streamGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<EntityProfile>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<EntityProfile>> input) throws Exception {
			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
			
			for (EntityProfile streamingEntity : input._2()) {
				String[] urlSplit = streamingEntity.getEntityUrl().split("/");
				Tuple2<String, String> pair;
				if (streamingEntity.isSource()) {
					pair = new Tuple2<String, String>("S" + streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());//"S" to source entities
				} else {
					pair = new Tuple2<String, String>("T" + streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());//"T" to source entities
				}
				
				output.add(pair);
			}
			
			return output.iterator();
		}
	});
    
    
    //coloca as tuplas no formato <e1, [b1,b2]>
    JavaPairDStream<String, Iterable<String>> entitySetBlocks = pairEntityBlock.groupByKey();
    
    
    //coloca as tuplas no formato <b1, (e1, b1, b2)>
    JavaPairDStream<String, List<String>> blockEntityAndAllBlocks = entitySetBlocks.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, List<String>>() {
		
    	@Override
		public Iterator<Tuple2<String, List<String>>> call(Tuple2<String, Iterable<String>> input) throws Exception {
			List<Tuple2<String, List<String>>> output = new ArrayList<Tuple2<String, List<String>>>();
			
			List<String> listOfBlocks = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			listOfBlocks.add(0, input._1());
			
			for (String block : input._2()) {
				Tuple2<String, List<String>> pair = new Tuple2<String, List<String>>(block, listOfBlocks);
				output.add(pair);
			}
			return output.iterator();
		}
	});
    
    
    //coloca as tuplas no formato <b1, [(e1, b1, b2), (e2, b1), (e3, b1, b2)]>
    JavaPairDStream<String, Iterable<List<String>>> blockPreprocessed = blockEntityAndAllBlocks.groupByKey();
    
    //print the entities in each stream
//    blockPreprocessed.foreachRDD(new VoidFunction<JavaPairRDD<String,Iterable<List<String>>>>() {
//		
//		@Override
//		public void call(JavaPairRDD<String, Iterable<List<String>>> t) throws Exception {
//			System.out.println("--------------New blocks---------------");
//			t.foreach(new VoidFunction<Tuple2<String,Iterable<List<String>>>>() {
//				
//				@Override
//				public void call(Tuple2<String, Iterable<List<String>>> t) throws Exception {
//					System.out.println(t);
//					
//				}
//			});
//			
//		}
//	});
    
    
    
 
    Function2<List<Iterable<List<String>>>, Optional<List<List<String>>>, Optional<List<List<String>>>> updateFunction =
            new Function2<List<Iterable<List<String>>>, Optional<List<List<String>>>, Optional<List<List<String>>>>() {
				@Override
				public Optional<List<List<String>>> call(List<Iterable<List<String>>> values,
						Optional<List<List<String>>> state) throws Exception {
					List<List<String>> count = state.or(new ArrayList<List<String>>());
					for (Iterable<List<String>> listBlocks : values) {
						List<List<String>> listOfBlocks = StreamSupport.stream(listBlocks.spliterator(), false).collect(Collectors.toList());
				    	count.addAll(listOfBlocks);
					}
			    	
					return Optional.of(count);
				}
    };
    
    //save in state
    JavaPairDStream<String, List<List<String>>> finalOutputProcessed =  blockPreprocessed.updateStateByKey(updateFunction);
    

    
    
//    //print the entities stored in state part1
//    JavaPairDStream<String, List<List<String>>> finalOutputProcessedDStream = finalOutputProcessed.transformToPair(new Function<JavaPairRDD<String,List<List<String>>>, JavaPairRDD<String, List<List<String>>>>() {
//
//		@Override
//		public JavaPairRDD<String, List<List<String>>> call(JavaPairRDD<String, List<List<String>>> v1) throws Exception {
//			return v1;
//		}
//	});
//    //print the entities stored in state part2
//    finalOutputProcessedDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,List<List<String>>>>() {
//		
//		@Override
//		public void call(JavaPairRDD<String, List<List<String>>> t) throws Exception {
//			System.out.println("--------------Accumulado blocks---------------");
//			t.foreach(new VoidFunction<Tuple2<String,List<List<String>>>>() {
//				
//				@Override
//				public void call(Tuple2<String, List<List<String>>> t) throws Exception {
//					System.out.println(t);
//					
//				}
//			});
//			
//		}
//	});
    
    
    
    //convert to JavaPairDStream
    JavaPairDStream<String, List<List<String>>> entityBlocksToCompare = finalOutputProcessed.filter(new Function<Tuple2<String,List<List<String>>>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, List<List<String>>> v1) throws Exception {
			return true;
		}
	});
    
    
    Accumulator<Integer> numberOfComparisons = sc.accumulator(0);
    
	//coloca as tuplas no formato <e1, e2 = 0.65> (calcula similaridade)
    JavaPairDStream<String, String> similarities = entityBlocksToCompare.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<List<String>>>, String, String>() {
    	
		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, List<List<String>>> input) throws Exception {
			
			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
			
			List<List<String>> listOfEntitiesToCompare = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			
			for (int i = 0; i < listOfEntitiesToCompare.size(); i++) {
				List<String> ent1 = listOfEntitiesToCompare.get(i);
				for (int j = i+1; j < listOfEntitiesToCompare.size(); j++) {
					List<String> ent2 = listOfEntitiesToCompare.get(j);
					if (ent1.get(0).charAt(0) != ent2.get(0).charAt(0)) {//compare only entities of different datasources
						
						if (ent1.size() >= 2 && ent2.size() >= 2) {
							String idEnt1 = ent1.get(0);
							String idEnt2 = ent2.get(0);
							double similarity = calculateSimilarity(ent1, ent2);
							numberOfComparisons.add(1);
							
							
							Tuple2<String, String> pair1 = new Tuple2<String, String>(idEnt1, idEnt2 + "=" + similarity);
							Tuple2<String, String> pair2 = new Tuple2<String, String>(idEnt2, idEnt1 + "=" + similarity);
							output.add(pair1);
							output.add(pair2);
						}
					}
					
				}
			}
			
			return output.iterator();
		}

		private double calculateSimilarity(List<String> ent1, List<String> ent2) {
//    			ent1.remove(0);
//    			ent2.remove(0);
			
			int maxSize = Math.max(ent1.size()-1, ent2.size()-1);
			List<String> intersect = new ArrayList<String>(ent1);
			intersect.retainAll(ent2);
			
			
			if (maxSize > 0) {
				double x = (double)intersect.size()/maxSize;
//				if (x>1) {
//					System.out.println();
//				}
				return x;
			} else {
				return 0;
			}
			
		}
	});
    
    //coloca as tuplas no formato <e1, [(e2 = 0.65), (e3 = 0.8)]>
    JavaPairDStream<String, Iterable<String>> similaritiesGrouped = similarities.groupByKey();
    
    
    
    
    //pruning phase
    JavaPairDStream<String, String> prunedOutput = similaritiesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
			
			double totalWeight = 0;
			double size = 0;
			
			for (String value : tuple._2()) {
				String[] entityWeight = value.split("\\=");
				totalWeight += Double.parseDouble(entityWeight[1]);
				size++;
			}
			
			double pruningWeight = totalWeight/size;
			
			for (String value : tuple._2()) {
				double weight = Double.parseDouble(value.split("\\=")[1]);
				if (weight >= pruningWeight) {
					output.add(new Tuple2<String, String>(tuple._1(), value));
				}
			}
			
			return output.iterator();
		}
	});
    
    JavaPairDStream<String, Iterable<String>> groupedPruned = prunedOutput.groupByKey();
    
    groupedPruned.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, String>() {

		@Override
		public Iterator<String> call(Tuple2<String, Iterable<String>> t) throws Exception {
			String out = "";
			List<String> listout = new ArrayList<String>();
			out += t._1();
			for (String string : t._2()) {
				out += string;
			}
			listout.add(out);
			return listout.iterator();
		}
	}).foreachRDD(rdd ->{
    	System.out.println("Batch size: " + rdd.count());
	      if(!rdd.isEmpty()){
//	    	 List<Tuple2<String, Iterable<String>>> x = rdd.collect();
	         rdd.saveAsTextFile("outputs/teste1/");
	      }
	});
    
    
//    groupedPruned.dstream().saveAsTextFiles("outputs/myoutput","txt");

    		
//    groupedPruned.foreachRDD(rdd -> {
//    	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
//        System.out.println("Batch size: " + rdd.count());
////        rdd.foreach(e -> System.out.println(e));
//    });
    
//    groupedPruned.foreachRDD(rdd ->{
//        if(!rdd.isEmpty()){
//           rdd.saveAsTextFile("outputs/teste1/");
//        }
//    });
    
    
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();
    ssc.awaitTermination();
    sc.stop();
    System.out.println("*** Streaming terminated");
  }

}
