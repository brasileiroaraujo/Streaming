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
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
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
public class PRIMEFastKafkaStructuredBlockControl {

public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = "outputs/abtbyStr1/";
	  int timeWindow = 20; //We have configured the period to x seconds (x sec).
	  final double MAX_SIZE = 1200;
	  
    
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
    
    
    // Generate running word count
    Dataset<EntityProfile> entities = lines.map(s -> new EntityProfile((String) s), Encoders.bean(EntityProfile.class));
    
	//INIT TIME
    long initTime = System.currentTimeMillis();
    LongAccumulator timeForEachInteration = spark.sparkContext().longAccumulator();
    timeForEachInteration.add(initTime);
//	DoubleAccumulator sumTimePerInterations = spark.sparkContext().doubleAccumulator();
//	DoubleAccumulator numberInterations = spark.sparkContext().doubleAccumulator();
    
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
    
    
    //State function
    MapGroupsWithStateFunction<Integer, Tuple2<Integer, Node>, NodeCollection, Tuple2<Integer, NodeCollection>> stateUpdateFunc =
    	      new MapGroupsWithStateFunction<Integer, Tuple2<Integer, Node>, NodeCollection, Tuple2<Integer, NodeCollection>>() {

				@Override
				public Tuple2<Integer, NodeCollection> call(Integer key, Iterator<Tuple2<Integer, Node>> values, GroupState<NodeCollection> state)
						throws Exception {
					NodeCollection count = (state.exists() ? state.get() : new NodeCollection());
					if (!count.isOnTheBlackList()) {
						List<Tuple2<Integer, Node>> listOfBlocks = IteratorUtils.toList(values);
	
	//					for (Node entBlocks : count.getNodeList()) {
	//						entBlocks.setMarked(false);
	//					}
	
						for (Tuple2<Integer, Node> entBlocks : listOfBlocks) {
							entBlocks._2().setMarked(true);
							count.add(entBlocks._2());
						}
						
						if (count.getNodeList().size() > (0.1 * MAX_SIZE)) {
							count.blackList();						
						}
					} 
//					else {
//						System.out.println();
//					}

					Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);
					state.update(count);

					return thisOne;
				}
    	      };
	
    //Save the entity blocks on State to be used in the nexts increments. 
    Dataset<Tuple2<Integer, NodeCollection>> storedBlocks = entityBlocks.mapGroupsWithState(
            stateUpdateFunc,
            Encoders.javaSerialization(NodeCollection.class),
            Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(NodeCollection.class)));
//            GroupStateTimeout.ProcessingTimeTimeout());//ESSA LINHA PODE SER AVALIADA DEPOIS
    
    
    
    DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    Dataset<Tuple2<Integer, Node>> pairEntityBlock = storedBlocks.flatMap(new FlatMapFunction<Tuple2<Integer, NodeCollection>, Tuple2<Integer, Node>>() {
		@Override
		public Iterator<Tuple2<Integer, Node>> call(Tuple2<Integer, NodeCollection> t) throws Exception {
			List<Node> entitiesToCompare = t._2().getNodeList();
			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
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
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class)));
    
    
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, Node>> graph = pairEntityBlock.groupByKey(new MapFunction<Tuple2<Integer, Node>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, Node> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    //Execute the pruning removing the low edges (entities in the Neighbors list)
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
    
    		
    		
    //Streaming query that stores the output incrementally.
//    StreamingQuery query = prunedGraph
////	  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//	  .writeStream()
//	  .outputMode("update")
//	  .format("kafka")
//	  .option("kafka.bootstrap.servers", "localhost:9092")
//	  .option("checkpointLocation", "checkpoints/")
//	  .option("topic", "outputTopic")
//	  .trigger(Trigger.ProcessingTime("15 seconds"))
//	  .start();
    
    // Start running the query that prints the running counts to the console
	StreamingQuery query = prunedGraph.writeStream()
		.outputMode("update")
		.format("console")
		// .format("parquet") // can be "orc", "json", "csv", etc.
		// .option("checkpointLocation", "checkpoints/")
		// .option("path", OUTPUT_PATH)
		.trigger(Trigger.ProcessingTime(timeWindow + " seconds"))
		.start();

    query.awaitTermination();

    
  }

}
