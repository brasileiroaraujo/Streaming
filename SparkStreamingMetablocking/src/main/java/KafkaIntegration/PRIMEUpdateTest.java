//package KafkaIntegration;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//import java.util.stream.StreamSupport;
//
//import org.apache.commons.collections.IteratorUtils;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.spark.Accumulator;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.Optional;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.FlatMapGroupsFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.Function3;
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.api.java.function.VoidFunction2;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoder;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.KeyValueGroupedDataset;
//import org.apache.spark.sql.RelationalGroupedDataset;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.State;
//import org.apache.spark.streaming.StateSpec;
//import org.apache.spark.streaming.Time;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.dstream.DStream;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import DataStructures.Attribute;
//import DataStructures.EntityProfile;
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//import streaming.util.CSVFileStreamGeneratorER;
//import streaming.util.CSVFileStreamGeneratorPMSD;
//import streaming.util.JavaDroppedWordsCounter;
//import streaming.util.JavaWordBlacklist;
//
//
////Parallel-based Metablockig for Streaming Data
//public class PRIMEUpdateTest {
//  public static void main(String[] args) throws InterruptedException {
//	  String OUTPUT_PATH = "outputs/gp-amazonUP/";
//	  int timeWindow = 12000; //We have configured the period to x seconds (x * 1000 ms).
//	  
//    //
//    // The "modern" way to initialize Spark is to create a SparkSession
//    // although they really come from the world of Spark SQL, and Dataset
//    // and DataFrame.
//    //
//    SparkSession spark = SparkSession
//        .builder()
//        .appName("streaming-Filtering")
//        .master("local[1]")
//        .getOrCreate();
//    
////    spark.sparkContext().getConf().set("spark.driver.memory", "4g");
//    
//    //
//    // Operating on a raw RDD actually requires access to the more low
//    // level SparkContext -- get the special Java version for convenience
//    //
//    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//    
//
//    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
//    // but it isn't -- it comes from org.apache.spark.streaming)
//    
//    
//    //Notice that Spark Streaming is not designed for periods shorter than about half a second. If you need a shorter delay in your processing, try Flink or Storm instead.
//    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(timeWindow));
//    //checkpointing is necessary since states are used
//    ssc.checkpoint("checkpoints/");
//    
//    
//    String bootstrapServers = "localhost:9092";
//    String subscribeType = "subscribe";
//    String topics = "mytopic";
//
//    //kafka pool to receive streaming data
//    Dataset<String> streamOfRecords = spark
//  	      .readStream()
//  	      .format("kafka")
//  	      .option("kafka.bootstrap.servers", bootstrapServers)
//  	      .option(subscribeType, topics)
//  	      .load()
//  	      .selectExpr("CAST(value AS STRING)")
//  	      .as(Encoders.STRING());
//    
//    
//    //INIT TIME
//    long initTime = System.currentTimeMillis();
//    Accumulator<Double> timeForEachInteration = sc.accumulator(initTime);
//    Accumulator<Double> sumTimePerInterations = sc.accumulator(0.0);
//    Accumulator<Integer> numberInterations = sc.accumulator(0);
//    
//    
//    Encoder<EntityProfile> entityEncoder = Encoders.javaSerialization(EntityProfile.class);
//    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
//    // to produce a stream of KeyAndValue objects
//    Dataset<EntityProfile> streamOfItems = streamOfRecords.map(s -> new EntityProfile(s), entityEncoder);
//    
//    Encoder<Tuple2<String, EntityProfile>> tupleEncoder = Encoders.tuple(Encoders.STRING(), entityEncoder);
//    Dataset<Tuple2<String, EntityProfile>> streamOfPairs = streamOfItems.flatMap(new FlatMapFunction<EntityProfile, Tuple2<String, EntityProfile>>() {
//
//		@Override
//		public Iterator call(EntityProfile se) throws Exception {
//			Set<Tuple2<String, EntityProfile>> output = new HashSet<Tuple2<String, EntityProfile>>();
//			
//			Set<String> cleanTokens = new HashSet<String>();
//			
//			for (Attribute att : se.getAttributes()) {
//				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
//				Collections.addAll(cleanTokens, tokens);
//			}
//			
//			for (String tk : cleanTokens) {
//				output.add(new Tuple2<>(tk, se));
//			}
//			
//			return output.iterator();
//		}
//	}, tupleEncoder);
//
//    
//    KeyValueGroupedDataset<String, Tuple2<String, EntityProfile>> streamGrouped = streamOfPairs.groupByKey(new MapFunction<Tuple2<String, EntityProfile>, String>() {
//
//		@Override
//		public String call(Tuple2<String, EntityProfile> value) throws Exception {
//			return value._1();
//		}
//	}, Encoders.STRING());
//
//    
//    //START THE METABLOCKING
//    //coloca as tuplas no formato <e1, b1>
//    Encoder<Tuple2<String, String>> tupleStringEncoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
//    Dataset<Tuple2<String, String>> pairEntityBlock = streamGrouped.flatMapGroups(new FlatMapGroupsFunction<String, Tuple2<String, EntityProfile>, Tuple2<String, String>>() {
//
//		@Override
//		public Iterator<Tuple2<String, String>> call(String key, Iterator<Tuple2<String, EntityProfile>> values)
//				throws Exception {
//			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
//			
//			List<Tuple2<String, EntityProfile>> entityTuples = IteratorUtils.toList(values);
//			
//			for (Tuple2<String, EntityProfile> tuple : entityTuples) {
//				EntityProfile streamingEntity = tuple._2();
//				String[] urlSplit = streamingEntity.getEntityUrl().split("/");
//				Tuple2<String, String> pair;
//				if (streamingEntity.isSource()) {
//					pair = new Tuple2<String, String>("S" + streamingEntity.getKey(), tuple._1());/*streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());*///"S" to source entities
//				} else {
//					pair = new Tuple2<String, String>("T" + streamingEntity.getKey(), tuple._1());/*streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());*///"T" to source entities
//				}
//				output.add(pair);
//			}
//			
//			return output.iterator();
//		}
//    	
//    }, tupleStringEncoder);
//    
//   
//    //coloca as tuplas no formato <e1, [b1,b2]>
//    KeyValueGroupedDataset<String, Tuple2<String, String>> entitySetBlocks = pairEntityBlock.groupByKey(new MapFunction<Tuple2<String, String>, String>() {
//
//		@Override
//		public String call(Tuple2<String, String> value) throws Exception {
//			return value._1();
//		}
//	}, Encoders.STRING());
//        
//    
//    //coloca as tuplas no formato <b1, (e1, b1, b2)>
//    Dataset<Tuple2<String, String>> blockEntityAndAllBlocks = entitySetBlocks.flatMapGroups(new FlatMapGroupsFunction<String, Tuple2<String, String>, Tuple2<String, String>>() {
//
//		@Override
//		public Iterator<Tuple2<String, String>> call(String key, Iterator<Tuple2<String, String>> values)
//				throws Exception {
//			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
//			String listOfBlocks = "";
//			listOfBlocks += key + ",";
//			List<Tuple2<String, String>> listIterable = IteratorUtils.toList(values);
//			for (Tuple2<String, String> tuple : listIterable) {
//				listOfBlocks += tuple._2() + ",";
//			}
//			
//			for (Tuple2<String, String> block : listIterable) {
//				Tuple2<String, String> pair = new Tuple2<String, String>(block._2(), listOfBlocks);
//				output.add(pair);
//			}
//			return output.iterator();
//		}
//    	
//    }, tupleStringEncoder);
//    
//    
//    //coloca as tuplas no formato <b1, [(e1, b1, b2), (e2, b1), (e3, b1, b2)]>
////    KeyValueGroupedDataset<String, Tuple2<String, String>> blockPreprocessed = blockEntityAndAllBlocks.groupByKey(new MapFunction<Tuple2<String, String>, String>() {
//
//		@Override
//		public String call(Tuple2<String, String> value) throws Exception {
//			return value._1();
//		}
//	}, Encoders.STRING());
//    
//    
//    //print the entities in each stream
////    blockPreprocessed.foreachRDD(new VoidFunction<JavaPairRDD<String,Iterable<List<String>>>>() {
////		
////		@Override
////		public void call(JavaPairRDD<String, Iterable<List<String>>> t) throws Exception {
////			System.out.println("--------------New blocks---------------");
////			t.foreach(new VoidFunction<Tuple2<String,Iterable<List<String>>>>() {
////				
////				@Override
////				public void call(Tuple2<String, Iterable<List<String>>> t) throws Exception {
////					System.out.println(t);
////					
////				}
////			});
////			
////		}
////	});
//    
//    
//    Broadcast<Integer> iterationCount = sc.broadcast(numberInterations.value());
//    Function3<String, Optional<Iterable<List<String>>>, State<List<List<String>>>, Tuple2<String, List<List<String>>>> 
//    			mappingFunctionBlockPreprocessed = (key, listBlocks, state) -> {
//    	List<List<String>> count = (state.exists() ? state.get() : new ArrayList<List<String>>());
//    	List<List<String>> listOfBlocks = StreamSupport.stream(listBlocks.get().spliterator(), false).collect(Collectors.toList());
//    	count.addAll(listOfBlocks);
//    	
//    	Tuple2<String, List<List<String>>> thisOne = new Tuple2<>(key, count);
//    	
//    	if (count.size() > 200 /*|| (count.size() == 1 && iterationCount.getValue()%5==0)*/) {
//    		state.remove();
//		} else {
//	        state.update(count);
//		}
////    	state.update(count);
//        
//        return thisOne;
//    };
//    
//    //save in state.
//    //Using mapWithState, we just manipulate the update entities/blocks. It's a property provided by mapWithState. UpdateState manipulates with all data (force brute).
//    JavaMapWithStateDStream<String, Iterable<List<String>>, List<List<String>>, Tuple2<String, List<List<String>>>> finalOutputProcessed =
//    		blockPreprocessed.mapWithState(StateSpec.function(mappingFunctionBlockPreprocessed));
//    
//    //Avoid the increasing of data in memory
//    finalOutputProcessed.checkpoint(new Duration(timeWindow));
//
//
////    //print the entities stored in state part1
////    DStream<Tuple2<String, List<List<String>>>> fin_Counts = finalOutputProcessed.dstream();
////    JavaDStream<Tuple2<String, List<List<String>>>> javaDStream = 
////    		   JavaDStream.fromDStream(fin_Counts,
////    		                                    scala.reflect.ClassTag$.MODULE$.apply(String.class));
////    //print the entities stored in state part2
////    javaDStream.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String,List<List<String>>>>>() {
////		
////		@Override
////		public void call(JavaRDD<Tuple2<String, List<List<String>>>> t) throws Exception {
////			System.out.println("--------------Accumulado Completo---------------");
////			t.foreach(new VoidFunction<Tuple2<String,List<List<String>>>>() {
////				
////				@Override
////				public void call(Tuple2<String, List<List<String>>> t) throws Exception {
////					System.out.println(t);
////					
////				}
////			});
////			
////		}
////	});
//    
//    
//    
//    
//    
//    
//    //convert to JavaPairDStream
//    JavaDStream<Tuple2<String, List<List<String>>>> onlyUpdatedEntityBlocksToCompare = finalOutputProcessed.filter(new Function<Tuple2<String,List<List<String>>>, Boolean>() {
//		@Override
//		public Boolean call(Tuple2<String, List<List<String>>> v1) throws Exception {
//			return true;
//		}
//	});
//    		
//    Accumulator<Integer> numberOfComparisons = sc.accumulator(0);
//    
//	//coloca as tuplas no formato <e1, e2 = 0.65> (calcula similaridade)
//    JavaPairDStream<String, String> similarities = onlyUpdatedEntityBlocksToCompare.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<List<String>>>, String, String>() {
//    	
//		@Override
//		public Iterator<Tuple2<String, String>> call(Tuple2<String, List<List<String>>> input) throws Exception {
//			
//			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
//			
//			List<List<String>> listOfEntitiesToCompare = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
//			
//			for (int i = 0; i < listOfEntitiesToCompare.size(); i++) {
//				List<String> ent1 = listOfEntitiesToCompare.get(i);
//				for (int j = i+1; j < listOfEntitiesToCompare.size(); j++) {
//					List<String> ent2 = listOfEntitiesToCompare.get(j);
//					if (ent1.get(0).charAt(0) != ent2.get(0).charAt(0) /*&& ent1.get(0).charAt(0) == 'S'*/) {//compare only entities of different datasources
//						
//						if (ent1.size() >= 2 && ent2.size() >= 2) {
//							String idEnt1 = ent1.get(0);
//							String idEnt2 = ent2.get(0);
//							double similarity = calculateSimilarity(ent1, ent2);
//							numberOfComparisons.add(1);
//							
//							if (ent1.get(0).charAt(0) == 'S') {
//								Tuple2<String, String> pair1 = new Tuple2<String, String>(idEnt1, idEnt2 + "=" + similarity);
//								output.add(pair1);
//							} else {
//								Tuple2<String, String> pair2 = new Tuple2<String, String>(idEnt2, idEnt1 + "=" + similarity);
//								output.add(pair2);
//							}
//							
//						}
//					}
//					
//				}
//			}
//			
//			return output.iterator();
//		}
//
//		private double calculateSimilarity(List<String> ent1, List<String> ent2) {
////    			ent1.remove(0);
////    			ent2.remove(0);
//			
//			int maxSize = Math.max(ent1.size()-1, ent2.size()-1);
//			List<String> intersect = new ArrayList<String>(ent1);
//			intersect.retainAll(ent2);
//			
//			
//			if (maxSize > 0) {
//				double x = (double)intersect.size()/maxSize;
////				if (x>1) {
////					System.out.println();
////				}
//				return x;
//			} else {
//				return 0;
//			}
//			
//		}
//	});
//    
//    //coloca as tuplas no formato <e1, [(e2 = 0.65), (e3 = 0.8)]>
//    JavaPairDStream<String, Iterable<String>> similaritiesGrouped = similarities.groupByKey();
//    
//    
//    
//    
//    //pruning phase
//    JavaPairDStream<String, String> prunedOutput = similaritiesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {
//
//		@Override
//		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
//			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
//			
//			double totalWeight = 0;
//			double size = 0;
//			
//			for (String value : tuple._2()) {
//				String[] entityWeight = value.split("\\=");
//				totalWeight += Double.parseDouble(entityWeight[1]);
//				size++;
//			}
//			
//			double pruningWeight = totalWeight/size;
//			
//			for (String value : tuple._2()) {
//				String[] entityValue = value.split("\\=");
//				String idEntity = entityValue[0];
//				double weight = Double.parseDouble(entityValue[1]);
//				if (weight >= pruningWeight) {
//					output.add(new Tuple2<String, String>(tuple._1(), idEntity));
//				}
//			}
//			
//			return output.iterator();
//		}
//	});
//    
//    JavaPairDStream<String, Iterable<String>> groupedPruned = prunedOutput.groupByKey();
//    
//    //save all results (including all pre calculated)
//    Function2<List<Iterable<String>>, Optional<List<String>>, Optional<List<String>>> updateOutputFunction =
//            new Function2<List<Iterable<String>>, Optional<List<String>>, Optional<List<String>>>() {
//				@Override
//				public Optional<List<String>> call(List<Iterable<String>> values,
//						Optional<List<String>> state) throws Exception {
//					List<String> count = state.or(new ArrayList<String>());
//					for (Iterable<String> listBlocks : values) {
//						List<String> listOfBlocks = StreamSupport.stream(listBlocks.spliterator(), false).collect(Collectors.toList());
//				    	count.addAll(listOfBlocks);
//					}
//			    	
//					return Optional.of(count);
//				}
//    };
//    
//    //save the output in state
//    JavaPairDStream<String, List<String>> PRIMEoutput =  groupedPruned.updateStateByKey(updateOutputFunction);
//    
//    //Avoid the increasing of data in memory
////    PRIMEoutput.checkpoint(new Duration(timeWindow));
//    
//    		
//    PRIMEoutput.foreachRDD(rdd ->{
//    	System.out.println("Batch size: " + rdd.count());
//    	
//    	//Total TIME
//    	long endTime = System.currentTimeMillis();
//    	System.out.println("Total time: " + ((double)endTime-initTime)/1000 + " seconds.");
//    	
//    	//Current Iteration TIME
//    	System.out.println("Iteration time: : " + ((double)endTime-timeForEachInteration.value())/1000 + " seconds.");
//    	
//    	sumTimePerInterations.add(((double)endTime-timeForEachInteration.value())/1000);
//    	numberInterations.add(1);
//    	System.out.println("Mean iteration time: " + (sumTimePerInterations.value()/numberInterations.value() + " seconds."));
//    	
//    	timeForEachInteration.setValue((double) endTime);
//    	
//    	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
//    	
//        if(!rdd.isEmpty()){
//           rdd.saveAsTextFile(OUTPUT_PATH);
//        }
//    });
//    
//    
////  groupedPruned.foreachRDD(rdd -> {
////	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
////    System.out.println("Batch size: " + rdd.count());
////    if(!rdd.isEmpty()){
////        rdd.saveAsTextFile(OUTPUT_PATH);
////    }
////  });
//    
//    
//    // start streaming
//    System.out.println("*** about to start streaming");
//    ssc.start();
//    ssc.awaitTermination();
//    System.out.println("*** Streaming terminated");
//  }
//
//}
