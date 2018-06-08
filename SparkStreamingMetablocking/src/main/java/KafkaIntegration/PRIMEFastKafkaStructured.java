package KafkaIntegration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.State;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.common.collect.Lists;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeCollection;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.BoxedUnit;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
public class PRIMEFastKafkaStructured {
  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
	  String OUTPUT_PATH = "outputs/abtbyStr1/";
	  int timeWindow = 10; //We have configured the period to x seconds (x sec).
	  
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
//    SparkSession spark = SparkSession
//        .builder()
//        .appName("streaming-Filtering")
//        .master("local[4]")
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
    
    SparkSession spark = SparkSession
    		  .builder()
    		  .appName("JavaStructuredNetworkWordCount")
    		  .master("local[4]")
    		  .getOrCreate();
    
    Dataset<String> lines = spark
    		  .readStream()
    		  .format("kafka")
    		  .option("kafka.bootstrap.servers", "localhost:9092")
    		  .option("subscribe", "mytopic")
    		  .load()
    		  .selectExpr("CAST(value AS STRING)")
    	      .as(Encoders.STRING());
    
//    lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    
    
    // Generate running word count
    Dataset<EntityProfile> entities = lines.map(s -> new EntityProfile((String) s), Encoders.bean(EntityProfile.class));
    
	//INIT TIME
    long initTime = System.currentTimeMillis();
    LongAccumulator timeForEachInteration = spark.sparkContext().longAccumulator();
    timeForEachInteration.add(initTime);
	DoubleAccumulator sumTimePerInterations = spark.sparkContext().doubleAccumulator();
	DoubleAccumulator numberInterations = spark.sparkContext().doubleAccumulator();
    
    Dataset<Tuple2<Integer, Node>> streamOfPairs = entities.flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, Node>>() {

		@Override
		public Iterator call(EntityProfile se) throws Exception {
			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
			
			Set<Integer> cleanTokens = new HashSet<Integer>();
			
			for (Attribute att : se.getAttributes()) {
//				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
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
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
    
    
	//define the blocks based on the tokens <tk, [n1,n2,n3]>, where n is a node.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, Node>> entityBlocks = streamOfPairs.groupByKey(new MapFunction<Tuple2<Integer, Node>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, Node> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    
    MapGroupsWithStateFunction<Integer, Tuple2<Integer, Node>, NodeCollection, Tuple2<Integer, NodeCollection>> stateUpdateFunc =
    	      new MapGroupsWithStateFunction<Integer, Tuple2<Integer, Node>, NodeCollection, Tuple2<Integer, NodeCollection>>() {

				@Override
				public Tuple2<Integer, NodeCollection> call(Integer key, Iterator<Tuple2<Integer, Node>> values, GroupState<NodeCollection> state)
						throws Exception {
					NodeCollection count = (state.exists() ? state.get() : new NodeCollection());
					List<Tuple2<Integer, Node>> listOfBlocks = IteratorUtils.toList(values);

					for (Node entBlocks : count.getNodeList()) {
						entBlocks.setMarked(false);
					}

					for (Tuple2<Integer, Node> entBlocks : listOfBlocks) {
						entBlocks._2().setMarked(true);
						count.add(entBlocks._2());
					}

					Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);
					state.update(count);

					return thisOne;
				}
    	      };
	
    Dataset<Tuple2<Integer, NodeCollection>> storedBlocks = entityBlocks.mapGroupsWithState(
            stateUpdateFunc,
            Encoders.javaSerialization(NodeCollection.class),
            Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(NodeCollection.class)),
            GroupStateTimeout.ProcessingTimeTimeout());//ESSA LINHA PODE SER AVALIADA DEPOIS
    
    
//    Function3<Integer, Iterator<Node>, GroupState<List<Node>>, Tuple2<Integer, List<Node>>> mappingFunctionBlockPreprocessed = (
//			key, listBlocks, state) -> {
//		// System.err.println("Chegou no State: " + ((System.currentTimeMillis() -
//		// initTime)/1000));
//		List<Node> count = (state.exists() ? state.get() : new ArrayList<Node>());
//		List<Node> listOfBlocks = new ArrayList<>();//StreamSupport.stream(listBlocks.get().spliterator(), false)
//				//.collect(Collectors.toList());
//
//		for (Node entBlocks : count) {
//			entBlocks.setMarked(false);
//		}
//
//		for (Node entBlocks : listOfBlocks) {
//			entBlocks.setMarked(true);
//			count.add(entBlocks);
//		}
//
//		Tuple2<Integer, List<Node>> thisOne = new Tuple2<>(key, count);
//		state.update(count);
//
//		return thisOne;
//	};
//	
//    Dataset<Node> processedDataset = entityBlocks.mapGroupsWithState(
//    		mappingFunctionBlockPreprocessed,
//    		Encoders.javaSerialization(List.class), 
//    		Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(List.class)));
    
    
//    entityBlocks.mapGroupsWithState(new MapGroupsWithStateFunction<Integer, Iterator<Tuple2<Integer,Node>>, List<Tuple2<Integer,Node>>, Tuple2<Integer, List<Tuple2<Integer,Node>>>>(){
//
//		@Override
//		public Tuple2<Integer, List<Tuple2<Integer, Node>>> call(Integer key,
//				Iterator<Iterator<Tuple2<Integer, Node>>> values, GroupState<List<Tuple2<Integer, Node>>> state)
//				throws Exception {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//    });
//    }
	
//    Dataset<Tuple2<Integer, List>> storedBlocks = entityBlocks.mapGroupsWithState(new MapGroupsWithStateFunction<Integer, Node, List, Tuple2<Integer,List>>(){
//		@Override
//		public Tuple2<Integer,List> call(Integer key, Iterator<Node> values, GroupState<List> state) throws Exception {
//			
//			List<Node> count = (state.exists() ? state.get() : new ArrayList<Node>());
//			List<Node> listOfBlocks = Lists.newArrayList(values);
//	
//			for (Node entBlocks : count) {
//				entBlocks.setMarked(false);
//			}
//	
//			for (Node entBlocks : listOfBlocks) {
//				entBlocks.setMarked(true);
//				count.add(entBlocks);
//			}
//	
//			Tuple2<Integer, List> thisOne = new Tuple2<>(key, count);
//			state.update(count);
//	
//			return thisOne;
//			
//		}}, Encoders.javaSerialization(List.class), Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(List.class)));
    
    
    DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    Dataset<Tuple2<Integer, Node>> pairEntityBlock = storedBlocks.flatMap(new FlatMapFunction<Tuple2<Integer, NodeCollection>, Tuple2<Integer, Node>>() {
		@Override
		public Iterator<Tuple2<Integer, Node>> call(Tuple2<Integer, NodeCollection> t) throws Exception {
			List<Node> entitiesToCompare = t._2().getNodeList();
			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
			for (int i = 0; i < entitiesToCompare.size(); i++) {
				Node n1 = entitiesToCompare.get(i);
				for (int j = i+1; j < entitiesToCompare.size(); j++) {
					Node n2 = entitiesToCompare.get(j);
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
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
    
//    Dataset<Tuple2<Integer, Node>> pairEntityBlock = entityBlocks.flatMapGroups(new FlatMapGroupsFunction<Integer, Tuple2<Integer, Node>, Tuple2<Integer, Node>>() {
//    	
//    	
//    	@Override
//		public Iterator<Tuple2<Integer, Node>> call(Integer key, Iterator<Tuple2<Integer, Node>> values)
//				throws Exception {
//			List<Tuple2<Integer, Node>> entitiesToCompare = IteratorUtils.toList(values);
//			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
//			for (int i = 0; i < entitiesToCompare.size(); i++) {
//				Node n1 = entitiesToCompare.get(i)._2();
//				for (int j = i+1; j < entitiesToCompare.size(); j++) {
//					Node n2 = entitiesToCompare.get(j)._2();
//					if (n1.isSource() != n2.isSource() /*&& (n1.isMarked() || n2.isMarked())*/) {//DESCOMENTAR
//						double similarity = calculateSimilarity(key, n1.getBlocks(), n2.getBlocks());
//						if (similarity >= 0) {
////							numberOfComparisons.add(1); DESCOMENTARRRR
//							if (n1.isSource()) {
//								n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
//							} else {
//								n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
//							}
//						}
//						
//					}
//				}
//			}
//			
//			for (Tuple2<Integer, Node> pair : entitiesToCompare) {
//				if (pair._2().isSource()) {
//					output.add(new Tuple2<Integer, Node>(pair._2().getId(), pair._2()));
//				}
//			}
//			return output.iterator();
//		}
//
//		private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
//			int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
//			Set<Integer> intersect = new HashSet<Integer>(ent1);
//			intersect.retainAll(ent2);
//
//			// MACOBI strategy
//			if (!Collections.min(intersect).equals(blockKey)) {
//				return -1;
//			}
//
//			if (maxSize > 0) {
//				double x = (double) intersect.size() / maxSize;
//				return x;
//			} else {
//				return 0;
//			}
//		}
//
//	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
    
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, Node>> graph = pairEntityBlock.groupByKey(new MapFunction<Tuple2<Integer, Node>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, Node> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    Dataset<String> prunedGraph = graph.mapGroups(new MapGroupsFunction<Integer, Tuple2<Integer, Node>, String>() {
    	
    	
    	@Override
		public String call(Integer key, Iterator<Tuple2<Integer, Node>> values)
				throws Exception {
    		List<Tuple2<Integer, Node>> nodes = IteratorUtils.toList(values);
			
			Node n1 = nodes.get(0)._2();//get the first node to merge with others.
			for (int j = 1; j < nodes.size(); j++) {
				Node n2 = nodes.get(j)._2();
				n1.addSetNeighbors(n2);
			}
			
			n1.pruning();
			return n1.getId() + "," + n1.toString();
		}


	}, Encoders.STRING());
    
    		
    		
//    prunedGraph.foreachPartition(rdd ->{
//    	//	System.err.println("Chegou no Fim: " + ((System.currentTimeMillis() - initTime)/1000));
////    		System.out.println("Batch size: " + rdd.count());
//    		
//    		//Total TIME
//    		long endTime = System.currentTimeMillis();
//    		System.out.println("Total time: " + ((double)endTime-initTime)/1000 + " seconds.");
//    		
//    		//Current Iteration TIME
//    		System.out.println("Iteration time: : " + ((double)endTime-timeForEachInteration.value())/1000 + " seconds.");
//    		
//    		sumTimePerInterations.add(((double)endTime-timeForEachInteration.value())/1000);
//    		numberInterations.add(1);
//    		System.out.println("Mean iteration time: " + (sumTimePerInterations.value()/numberInterations.value() + " seconds."));
//    		
//    		timeForEachInteration.setValue((long) endTime);
//    		
//    		System.out.println("Number of Comparisons: " + numberOfComparisons.value());
//    	});

    // Start running the query that prints the running counts to the console
//    StreamingQuery query = prunedGraph.writeStream()
//      .outputMode("update")
//      .format("console")
////      .format("parquet")        // can be "orc", "json", "csv", etc.
////      .option("checkpointLocation", "checkpoints/")
////      .option("path", OUTPUT_PATH)
//      .trigger(Trigger.ProcessingTime(timeWindow + " seconds"))
//      .start();
    
    StreamingQuery query = prunedGraph
//	  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
	  .writeStream()
	  .outputMode("update")
	  .format("kafka")
	  .option("kafka.bootstrap.servers", "localhost:9092")
	  .option("checkpointLocation", "checkpoints/")
	  .option("topic", "outputTopic")
	  .start();

    query.awaitTermination();

    
  }

}
