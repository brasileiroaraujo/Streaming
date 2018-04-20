package streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import scala.Tuple2;
import streaming.util.AccumulatorParamSet;
import streaming.util.CSVFileStreamGeneratorPMSD;


//Parallel-based Metablockig for Streaming Data
public class PMSD2 {
  public static void main(String[] args) {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
        .master("local[4]")
        .getOrCreate();

    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));

    String checkpointPath = File.separator + "tmp" + File.separator + "LSWJ" + File.separator + "checkpoints";
    File checkpointDir = new File(checkpointPath);
    checkpointDir.mkdir();
    checkpointDir.deleteOnExit();
    ssc.checkpoint(checkpointPath);

    // use the utility class to produce a sequence of 10 files, each containing 5(100) records
    CSVFileStreamGeneratorPMSD fm = new CSVFileStreamGeneratorPMSD("inputs/dataset2_gp", 1, 5, 300);
    // create the stream, which will contain the rows of the individual files as strings
    // -- notice we can create the stream even though this directory won't have any data until we call
    // fm.makeFiles() below
    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<EntityProfile> streamOfItems = streamOfRecords.map(s -> new EntityProfile(s, ";"));
    

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

//    Function3<String, Optional<StreamingEntity>, State<List<StreamingEntity>>, Tuple2<String, List<StreamingEntity>>>  mappingFunction = (category, item, state) -> {
//    	List<StreamingEntity> count = (state.exists() ? state.get() : new ArrayList<StreamingEntity>());
//    	count.add(item.get());
//        Tuple2<String, List<StreamingEntity>> thisOne = new Tuple2<>(category, count);
//        state.update(count);
//        return thisOne;
//    };
//    
//    //save in state
//    JavaMapWithStateDStream<String, StreamingEntity, List<StreamingEntity>, Tuple2<String, List<StreamingEntity>>> streamTokenBlocking =
//        streamOfPairs.mapWithState(StateSpec.function(mappingFunction));
    

//    streamTokenBlocking.foreachRDD(rdd -> {
//      System.out.println("Batch size: " + rdd.count());
//      rdd.foreach(e -> System.out.println(e));
//    });
    
    //START THE METABLOCKING
    //coloca as tuplas no formato <e1, b1>
    JavaPairDStream<String, String> pairEntityBlock = streamGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<EntityProfile>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<EntityProfile>> input) throws Exception {
			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
			
			for (EntityProfile streamingEntity : input._2()) {
				String[] urlSplit = streamingEntity.getEntityUrl().split("/");
				Tuple2<String, String> pair = new Tuple2<String, String>(streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());
				output.add(pair);
			}
			
			return output.iterator();
		}
	});
    
//    pairEntityBlock.foreachRDD(rdd -> {
//      System.out.println("Batch size: " + rdd.count());
//      rdd.foreach(e -> System.out.println(e));
//    });
    
    //coloca as tuplas no formato <e1, [b1,b2]>
    JavaPairDStream<String, Iterable<String>> entitySetBlocks = pairEntityBlock.groupByKey();
    
    //list of blocks that will be updated
    Accumulator<HashSet<String>> blocksUpdated = sc.accumulator(new HashSet<String>(), new AccumulatorParamSet());
    
    
    //coloca as tuplas no formato <b1, (e1, b1, b2)>
    JavaPairDStream<String, List<String>> blockEntityAndAllBlocks = entitySetBlocks.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, List<String>>() {

		@Override
		public Iterator<Tuple2<String, List<String>>> call(Tuple2<String, Iterable<String>> input) throws Exception {
			List<Tuple2<String, List<String>>> output = new ArrayList<Tuple2<String, List<String>>>();
			
			List<String> listOfBlocks = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			listOfBlocks.add(0, input._1());
			
			HashSet<String> setB = new HashSet<String>();
			for (String block : input._2()) {
				Tuple2<String, List<String>> pair = new Tuple2<String, List<String>>(block, listOfBlocks);
				setB.add(block);
				output.add(pair);
			}
			blocksUpdated.add(setB);
			return output.iterator();
		}
	});
    
    //coloca as tuplas no formato <b1, [(e1, b1, b2), (e2, b1), (e3, b1, b2)]>
    JavaPairDStream<String, Iterable<List<String>>> blockPreprocessed = blockEntityAndAllBlocks.groupByKey();
    
    
    Broadcast<HashSet<String>> broadcastBlocksUpdated = sc.broadcast(blocksUpdated.value());
//    blocksUpdated.setValue(new HashSet<String>());
    
    
    Function3<String, Optional<Iterable<List<String>>>, State<List<List<String>>>, Tuple2<String, List<List<String>>>> 
    			mappingFunctionBlockPreprocessed = (key, listBlocks, state) -> {
    	List<List<String>> count = (state.exists() ? state.get() : new ArrayList<List<String>>());
    	List<List<String>> listOfBlocks = StreamSupport.stream(listBlocks.get().spliterator(), false).collect(Collectors.toList());
    	count.addAll(listOfBlocks);
        Tuple2<String, List<List<String>>> thisOne = new Tuple2<>(key, count);
        state.update(count);
        return thisOne;
    };
    
    //save in state
    JavaMapWithStateDStream<String, Iterable<List<String>>, List<List<String>>, Tuple2<String, List<List<String>>>> finalOutputProcessed =
    		blockPreprocessed.mapWithState(StateSpec.function(mappingFunctionBlockPreprocessed));
    
    
    //select only the updated blocks (in this turn)
    JavaDStream<Tuple2<String, List<List<String>>>> onlyUpdatedBlocks = finalOutputProcessed.filter(new Function<Tuple2<String,List<List<String>>>, Boolean>() {
		
		@Override
		public Boolean call(Tuple2<String, List<List<String>>> tuple) throws Exception {
			HashSet<String> x = broadcastBlocksUpdated.getValue();
			System.out.println(x);
			if (broadcastBlocksUpdated.getValue().contains(tuple._1())) {
				return true;
			}
			return false;
		}
	});
    
    
    
//    finalOutputProcessed.foreachRDD(rdd -> {
//        System.out.println("Batch size: " + rdd.count());
//        rdd.foreach(e -> System.out.println(e));
//      });
    
    
	//coloca as tuplas no formato <e1, e2 = 0.65> (calcula similaridade)
    JavaPairDStream<String, String> similarities = onlyUpdatedBlocks.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<List<String>>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, List<List<String>>> input) throws Exception {
			
			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
			
			List<List<String>> listOfEntitiesToCompare = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			
			for (int i = 0; i < listOfEntitiesToCompare.size(); i++) {
				List<String> ent1 = listOfEntitiesToCompare.get(i);
				for (int j = i+1; j < listOfEntitiesToCompare.size(); j++) {
					List<String> ent2 = listOfEntitiesToCompare.get(j);
					if (ent1.size() >= 2 && ent2.size() >= 2) {
						String idEnt1 = ent1.get(0);
						String idEnt2 = ent2.get(0);
						double similarity = calculateSimilarity(ent1, ent2);
						
						
						Tuple2<String, String> pair1 = new Tuple2<String, String>(idEnt1, idEnt2 + "=" + similarity);
						Tuple2<String, String> pair2 = new Tuple2<String, String>(idEnt2, idEnt1 + "=" + similarity);
						output.add(pair1);
						output.add(pair2);
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
				if (x>1) {
					System.out.println();
				}
				return x;
			} else {
				return 0;
			}
			
		}
	});
    
    //coloca as tuplas no formato <e1, [(e2 = 0.65), (e3 = 0.8)]>
    JavaPairDStream<String, Iterable<String>> similaritiesGrouped = similarities.groupByKey();
    
    
//    similaritiesGrouped.foreachRDD(rdd -> {
//      System.out.println("Batch size: " + rdd.count());
//      rdd.foreach(e -> System.out.println(e));
//    });
    
//    //coloca as tuplas no formato <e1, e2 = 0.65> (calcula similaridade)
//    JavaPairDStream<String, String> similarities = blockPreprocessed.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<List<String>>>, String, String>() {
//
//		@Override
//		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<List<String>>> input) throws Exception {
//			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
//			
//			List<List<String>> listOfEntitiesToCompare = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
//			
//			for (int i = 0; i < listOfEntitiesToCompare.size(); i++) {
//				List<String> ent1 = listOfEntitiesToCompare.get(i);
//				for (int j = i+1; j < listOfEntitiesToCompare.size(); j++) {
//					List<String> ent2 = listOfEntitiesToCompare.get(j);
//					if (ent1.size() >= 2 && ent2.size() >= 2) {
//						String idEnt1 = ent1.get(0);
//						String idEnt2 = ent2.get(0);
//						double similarity = calculateSimilarity(ent1, ent2);
//						Tuple2<String, String> pair = new Tuple2<String, String>(idEnt1, idEnt2 + " = " + similarity);
//						output.add(pair);
//					}
//					
//				}
//			}
//			
//			return output.iterator();
//		}
//
//		private double calculateSimilarity(List<String> ent1, List<String> ent2) {
////			ent1.remove(0);
////			ent2.remove(0);
//			
//			int maxSize = Math.max(ent1.size()-1, ent2.size()-1);
//			List<String> intersect = new ArrayList<String>(ent1);
//			intersect.retainAll(ent2);
//			if (maxSize > 0) {
//				return intersect.size()/maxSize;
//			} else {
//				return 0;
//			}
//			
//		}
//	});
    
    //salva as similaridades na memória (acumulativa) e imprime.
//    Function3<String, Optional<String>, State<List<String>>, Tuple2<String, List<String>>>  mappingFunctionSimilarities = (key, similarity, state) -> {
//    	List<String> count = (state.exists() ? state.get() : new ArrayList<String>());
//    	count.add(similarity.get());
//        Tuple2<String, List<String>> thisOne = new Tuple2<>(key, count);
//        state.update(count);
//        return thisOne;
//    };
//    
//    //save in state
//    JavaMapWithStateDStream<String, String, List<String>, Tuple2<String, List<String>>> finalOutputSimilarities =
//    		similarities.mapWithState(StateSpec.function(mappingFunctionSimilarities));
//    
//    
//    finalOutputSimilarities.foreachRDD(rdd -> {
//        System.out.println("Batch size: " + rdd.count());
//        rdd.foreach(e -> System.out.println(e));
//      });
    	
    	
    
//    similarities.foreachRDD(rdd -> {
//      System.out.println("Batch size: " + rdd.count());
//      rdd.foreach(e -> System.out.println(e));
//    });
    
    
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
					if (weight == 0) {
						System.out.println();
					}
					output.add(new Tuple2<String, String>(tuple._1(), value));
				}
			}
			
			return output.iterator();
		}
	});
    
    JavaPairDStream<String, Iterable<String>> groupedPruned = prunedOutput.groupByKey();
    		
    groupedPruned.foreachRDD(rdd -> {
        System.out.println("Batch size: " + rdd.count());
        rdd.foreach(e -> System.out.println(e));
      });
    
    
    
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();


    Thread t = new Thread() {
      public void run() {
    	 try {
              // A curious fact about files based streaming is that any files written
              // before the first RDD is produced are ignored. So wait longer than
              // that before producing files.
              Thread.sleep(2000);

              System.out.println("*** producing data");
              // start producing files
              fm.makeFiles();

              // give it time to get processed
              Thread.sleep(2000);

              fm.makeFiles();
              
              // give it time to get processed
              Thread.sleep(2000);

              fm.makeFiles();

              // give it time to get processed
              Thread.sleep(10000);
        } catch (InterruptedException ie) {
        } catch (IOException ioe) {
          throw new RuntimeException("problem in background thread", ioe);
        }
        ssc.stop();
        System.out.println("*** stopping streaming");
      }
    };
    t.start();

    try {
      ssc.awaitTermination();
    } catch (InterruptedException ie) {

    }
    System.out.println("*** Streaming terminated");
  }

}
