package KafkaIntegration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

import org.apache.commons.collections.IteratorUtils;
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
import org.w3c.dom.NodeList;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeCollection;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import streaming.util.CSVFileStreamGeneratorER;
import streaming.util.CSVFileStreamGeneratorPMSD;
import streaming.util.JavaDroppedWordsCounter;
import streaming.util.JavaWordBlacklist;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PRIMEStreamingWindowed {
  public static void main(String[] args) throws InterruptedException {
	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = "C:\\Users\\lutibr\\Documents\\outputs\\gp-amazonUP2";
	  int timeWindow = Integer.parseInt(args[0]) * 1000; //We have configured the period to x seconds (x * 1000 ms).
	  
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("PRIMEStreamingWindowed")
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
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(timeWindow));
    //checkpointing is necessary since states are used
    ssc.checkpoint("C:\\Users\\lutibr\\Documents\\checkpoint");
    

    //kafka pool to receive streaming data
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", args[1]);//"localhost:9092"
    Set<String> topics = Collections.singleton("mytopic");

    JavaPairInputDStream<String, String> streamOfRecords = KafkaUtils.createDirectStream(ssc,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
    
    
    //INIT TIME
    long initTime = System.currentTimeMillis();
    Accumulator<Double> timeForEachInteration = sc.accumulator(initTime);
    Accumulator<Double> sumTimePerInterations = sc.accumulator(0.0);
    Accumulator<Integer> numberInterations = sc.accumulator(0);
    
    
    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<EntityProfile> streamOfItems = streamOfRecords.map(s -> new EntityProfile(s._2()));
    

    JavaPairDStream<Integer, Node> streamOfPairs =
        streamOfItems.flatMapToPair(new PairFlatMapFunction<EntityProfile, Integer, Node>() {
			@Override
			public Iterator<Tuple2<Integer, Node>> call(EntityProfile se) throws Exception {
				List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
				
				Set<Integer> cleanTokens = new HashSet<Integer>();
				
				for (Attribute att : se.getAttributes()) {
//					String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
					KeywordGenerator kw = new KeywordGeneratorImpl();
					for (String string : kw.generateKeyWords(att.getValue())) {
						cleanTokens.add(string.hashCode());
					}
				}
				
				Node node = new Node(se.getKey(), cleanTokens, new HashSet<>(), se.isSource());
				
				for (Integer tk : cleanTokens) {
					node.setTokenTemporary(tk);
					output.add(new Tuple2<Integer, Node>(tk, node));
				}
				
				return output.iterator();
			}
		});
    
    //define the blocks based on the tokens <tk, [n1,n2,n3]>, where n is a node.
    JavaPairDStream<Integer, Iterable<Node>> streamGrouped = streamOfPairs.groupByKey();
    
    
    
	Function3<Integer, Optional<Iterable<Node>>, State<NodeCollection>, Tuple2<Integer, NodeCollection>> mappingFunctionBlockPreprocessed = (
			key, listBlocks, state) -> {
			NodeCollection count = (state.exists() ? state.get() : new NodeCollection());
			List<Node> listOfBlocks = StreamSupport.stream(listBlocks.get().spliterator(), false).collect(Collectors.toList());
			
			count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds
			
			for (Node entBlocks : count.getNodeList()) {
				entBlocks.setMarked(false);
			}

			for (Node nd : listOfBlocks) {
				nd.setMarked(true);
				count.add(nd);
			}

			Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);
			state.update(count);

			return thisOne;
	};

		// save in state.
		// Using mapWithState, we just manipulate the update entities/blocks. It's a
		// property provided by mapWithState. UpdateState manipulates with all data
		// (force brute).
	JavaMapWithStateDStream<Integer, Iterable<Node>, NodeCollection, Tuple2<Integer, NodeCollection>> finalOutputProcessed = streamGrouped
				.mapWithState(StateSpec.function(mappingFunctionBlockPreprocessed));
	
	//Avoid the increasing of data in memory
    finalOutputProcessed.checkpoint(new Duration(timeWindow*3));
    
    //convert to JavaPairDStream
    JavaDStream<Tuple2<Integer, NodeCollection>> onlyUpdatedEntityBlocksToCompare = finalOutputProcessed.filter(new Function<Tuple2<Integer,NodeCollection>, Boolean>() {
		@Override
		public Boolean call(Tuple2<Integer, NodeCollection> arg0) throws Exception {
			return true;
		}
	});
    
    
    Accumulator<Integer> numberOfComparisons = sc.accumulator(0);
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    JavaPairDStream<Integer, Node> similarities = onlyUpdatedEntityBlocksToCompare.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,NodeCollection>, Integer, Node>() {
    	@Override
		public Iterator<Tuple2<Integer, Node>> call(Tuple2<Integer, NodeCollection> t) throws Exception {
    		List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
    		List<Node> entitiesToCompare = t._2().getNodeList();
			//List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
			for (int i = 0; i < entitiesToCompare.size(); i++) {
				Node n1 = entitiesToCompare.get(i);
				for (int j = i+1; j < entitiesToCompare.size(); j++) {
					Node n2 = entitiesToCompare.get(j);
					//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
					if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
						double similarity = calculateSimilarity(t._1(), n1.getBlocks(), n2.getBlocks());
						if (similarity >= 0) {
							numberOfComparisons.add(1);
							if (n1.isSource()) {
								n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
							} else {
								n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
							}
						}
						
					}
				}
			}
			
			for (Node node : entitiesToCompare) {
				if (node.isSource()) {
					output.add(new Tuple2<Integer, Node>(node.getId(), node));
				}
			}
			return output.iterator();
		}

		private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
			int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
			Set<Integer> intersect = new HashSet<Integer>(ent1);
			intersect.retainAll(ent2);

			// MACOBI strategy
			if (!Collections.min(intersect).equals(blockKey)) {
				return -1;
			}

			if (maxSize > 0) {
				double x = (double) intersect.size() / maxSize;
				return x;
			} else {
				return 0;
			}
		}
	});
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    JavaPairDStream<Integer, Iterable<Node>> graph = similarities.groupByKey();
    
    
    //Execute the pruning removing the low edges (entities in the Neighbors list)
    JavaDStream<String> prunedGraph = graph.map(new Function<Tuple2<Integer,Iterable<Node>>, String>() {

		@Override
		public String call(Tuple2<Integer, Iterable<Node>> values) throws Exception {
			List<Node> nodes = StreamSupport.stream(values._2().spliterator(), false).collect(Collectors.toList());
			
			Node n1 = nodes.get(0);//get the first node to merge with others.
			for (int j = 1; j < nodes.size(); j++) {
				Node n2 = nodes.get(j);
				n1.addSetNeighbors(n2);
			}
			
			n1.pruning();
			return n1.getId() + "," + n1.toString();
		}
	});
    
    
    //save all results (including all pre calculated)
    /*Function2<List<Iterable<String>>, Optional<List<String>>, Optional<List<String>>> updateOutputFunction =
            new Function2<List<Iterable<String>>, Optional<List<String>>, Optional<List<String>>>() {
				@Override
				public Optional<List<String>> call(List<Iterable<String>> values,
						Optional<List<String>> state) throws Exception {
					List<String> count = state.or(new ArrayList<String>());
					for (Iterable<String> listBlocks : values) {
						List<String> listOfBlocks = StreamSupport.stream(listBlocks.spliterator(), false).collect(Collectors.toList());
				    	count.addAll(listOfBlocks);
					}
			    	
					return Optional.of(count);
				}
    };
    
    //save the output in state
    JavaPairDStream<String, List<String>> PRIMEoutput =  groupedPruned.updateStateByKey(updateOutputFunction);
    
    //Avoid the increasing of data in memory
    PRIMEoutput.checkpoint(new Duration(timeWindow*3));*/
    
    		
    prunedGraph.foreachRDD(rdd ->{
//    	System.err.println("Chegou no Fim: " + ((System.currentTimeMillis() - initTime)/1000));
    	System.out.println("Batch size: " + rdd.count());
    	
    	//Total TIME
    	long endTime = System.currentTimeMillis();
    	System.out.println("Total time: " + ((double)endTime-initTime)/1000 + " seconds.");
    	
    	//Current Iteration TIME
    	System.out.println("Iteration time: : " + ((double)endTime-timeForEachInteration.value())/1000 + " seconds.");
    	
    	sumTimePerInterations.add(((double)endTime-timeForEachInteration.value())/1000);
    	numberInterations.add(1);
    	System.out.println("Mean iteration time: " + (sumTimePerInterations.value()/numberInterations.value() + " seconds."));
    	
    	timeForEachInteration.setValue((double) endTime);
    	
    	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
    	
        if(!rdd.isEmpty()){
           rdd.saveAsTextFile(OUTPUT_PATH);
        }
    });
    
    
//  groupedPruned.foreachRDD(rdd -> {
//	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
//    System.out.println("Batch size: " + rdd.count());
//    if(!rdd.isEmpty()){
//        rdd.saveAsTextFile(OUTPUT_PATH);
//    }
//  });
    
    
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();
    ssc.awaitTermination();
    System.out.println("*** Streaming terminated");
  }

}
