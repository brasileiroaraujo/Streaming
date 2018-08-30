package PrimeApproach;

import java.io.IOException;
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
import org.apache.spark.sql.execution.streaming.ProgressReporter;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent;
import org.apache.spark.streaming.State;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.common.collect.Lists;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.MetablockingNode;
import DataStructures.MetablockingNodeCollection;
import DataStructures.Node;
import DataStructures.NodeCollection;
import DataStructures.SingletonFileWriter;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.BoxedUnit;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class MetablockingStructured {
  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = args[3];  //$$ will be replaced by the increment index //"outputs/teste.txt";
	  int timeWindow = Integer.parseInt(args[0]); //We have configured the period to x seconds (x sec).
	  
    
    SparkSession spark = SparkSession
    		  .builder()
    		  .appName("MetablockingStructured")
//    		  .master("local[6]")
    		  .getOrCreate();
    
    Dataset<String> lines = spark
    		  .readStream()
    		  .format("kafka")
    		  .option("kafka.bootstrap.servers", args[1])//localhost:9092    10.171.171.50:8088
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
    
    Dataset<Tuple2<Integer, MetablockingNode>> streamOfPairs = entities.flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, MetablockingNode>>() {

		@Override
		public Iterator call(EntityProfile se) throws Exception {
			List<Tuple2<Integer, MetablockingNode>> output = new ArrayList<Tuple2<Integer, MetablockingNode>>();
			
			Set<Integer> cleanTokens = new HashSet<Integer>();
			
			for (Attribute att : se.getAttributes()) {
//				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
				KeywordGenerator kw = new KeywordGeneratorImpl();
				for (String string : kw.generateKeyWords(att.getValue())) {
					cleanTokens.add(string.hashCode());
				}
			}
			
			MetablockingNode node = new MetablockingNode(se.getKey(), se.isSource());
			
			for (Integer tk : cleanTokens) {
				output.add(new Tuple2<Integer, MetablockingNode>(tk, node));
			}
			
			return output.iterator();
		}
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(MetablockingNode.class)));
    
    
	//define the blocks based on the tokens <tk, [n1,n2,n3]>, where n is a node.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, MetablockingNode>> entityBlocks = streamOfPairs.groupByKey(new MapFunction<Tuple2<Integer, MetablockingNode>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, MetablockingNode> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    
    //coloca as tuplas no formato <e1, b1>
    Dataset<Tuple2<String, Integer>> pairEntityBlock = entityBlocks.flatMapGroups(new FlatMapGroupsFunction() {

		@Override
		public Iterator<Tuple2<String, Integer>> call(Object arg0, Iterator arg1) throws Exception {
			List<Tuple2<Integer, MetablockingNode>> values = Lists.newArrayList(arg1);
			ArrayList<Tuple2<String, Integer>> output = new ArrayList<Tuple2<String, Integer>>();
			
			for (Tuple2<Integer, MetablockingNode> e : values) {
				if (e._2().isSource()) {
					output.add(new Tuple2<String, Integer>("S" + e._2().getEntityId(), e._1));
				} else {
					output.add(new Tuple2<String, Integer>("T" + e._2().getEntityId(), e._1));
				}
			}
			return output.iterator();
		}
	}, Encoders.tuple(Encoders.STRING(), Encoders.INT()));
    
    
    //coloca as tuplas no formato <e1, [b1,b2]>
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> entitySetBlocks = pairEntityBlock.groupByKey(new MapFunction<Tuple2<String, Integer>, String>() {

		@Override
		public String call(Tuple2<String, Integer> n) throws Exception {
			return n._1();
		}
	}, Encoders.STRING());
    
    
    
    //coloca as tuplas no formato <b1, (e1, b1, b2)>
    Dataset<Tuple2<Integer, MetablockingNode>> blockEntityAndAllBlocks = entitySetBlocks.flatMapGroups(new FlatMapGroupsFunction() {

		@Override
		public Iterator<Tuple2<Integer, MetablockingNode>> call(Object arg0, Iterator arg1) throws Exception {
			List<Tuple2<String, Integer>> values = Lists.newArrayList(arg1);
			ArrayList<Tuple2<Integer, MetablockingNode>> output = new ArrayList<Tuple2<Integer, MetablockingNode>>();
			
			String key = (String) arg0;
			MetablockingNode metaNode;
			if (key.charAt(0) == 'S') {
				metaNode = new MetablockingNode(Integer.parseInt(key.substring(1, key.length())), true);
			} else {
				metaNode = new MetablockingNode(Integer.parseInt(key.substring(1, key.length())), false);
			}
			 
			for (Tuple2<String, Integer> entBlocks : values) {
				metaNode.addInBlocks(entBlocks._2());
			}
			
			for (Tuple2<String, Integer> entBlocks : values) {
				output.add(new Tuple2<Integer, MetablockingNode>(entBlocks._2(), metaNode));
			}
			return output.iterator();
		}
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(MetablockingNode.class)));
    
    
    
    //coloca as tuplas no formato <b1, [(e1, b1, b2), (e2, b1), (e3, b1, b2)]>
    KeyValueGroupedDataset<Integer, Tuple2<Integer, MetablockingNode>> blockPreprocessed = blockEntityAndAllBlocks.groupByKey(new MapFunction<Tuple2<Integer, MetablockingNode>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, MetablockingNode> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    
    //State function
    MapGroupsWithStateFunction<Integer, Tuple2<Integer, MetablockingNode>, MetablockingNodeCollection, Tuple2<Integer, MetablockingNodeCollection>> stateUpdateFunc =
    	      new MapGroupsWithStateFunction<Integer, Tuple2<Integer, MetablockingNode>, MetablockingNodeCollection, Tuple2<Integer, MetablockingNodeCollection>>() {

				@Override
				public Tuple2<Integer, MetablockingNodeCollection> call(Integer key, Iterator<Tuple2<Integer, MetablockingNode>> values, GroupState<MetablockingNodeCollection> state)
						throws Exception {
					MetablockingNodeCollection count = (state.exists() ? state.get() : new MetablockingNodeCollection());
					List<Tuple2<Integer, MetablockingNode>> listOfBlocks = IteratorUtils.toList(values);
					
					//count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds
					
					for (MetablockingNode entBlocks : count.getNodeList()) {
						entBlocks.setMarked(false);
					}

					for (Tuple2<Integer, MetablockingNode> entBlocks : listOfBlocks) {
						entBlocks._2().setMarked(true);
						count.add(entBlocks._2());
					}

					Tuple2<Integer, MetablockingNodeCollection> thisOne = new Tuple2<>(key, count);
					state.update(count);

					return thisOne;
				}
    	      };
	
    //Save the entity blocks on State to be used in the nexts increments. 
    Dataset<Tuple2<Integer, MetablockingNodeCollection>> storedBlocks = blockPreprocessed.mapGroupsWithState(
            stateUpdateFunc,
            Encoders.javaSerialization(MetablockingNodeCollection.class),
            Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(MetablockingNodeCollection.class)));
//            GroupStateTimeout.ProcessingTimeTimeout());//ESSA LINHA PODE SER AVALIADA DEPOIS
    
    
    
    
    
    DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    Dataset<Tuple2<Integer, MetablockingNode>> similarities = storedBlocks.flatMap(new FlatMapFunction<Tuple2<Integer, MetablockingNodeCollection>, Tuple2<Integer, MetablockingNode>>() {
		@Override
		public Iterator<Tuple2<Integer, MetablockingNode>> call(Tuple2<Integer, MetablockingNodeCollection> t) throws Exception {
			List<MetablockingNode> entitiesToCompare = t._2().getNodeList();
			List<Tuple2<Integer, MetablockingNode>> output = new ArrayList<Tuple2<Integer, MetablockingNode>>();
			for (int i = 0; i < entitiesToCompare.size(); i++) {
				MetablockingNode n1 = entitiesToCompare.get(i);
				for (int j = i+1; j < entitiesToCompare.size(); j++) {
					MetablockingNode n2 = entitiesToCompare.get(j);
					//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
					if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
						double similarity = calculateSimilarity(t._1(), n1.getBlocks(), n2.getBlocks());
						if (similarity >= 0) {
							numberOfComparisons.add(1);
							if (n1.isSource()) {
								n1.addNeighbor(new Tuple2<Integer, Double>(n2.getEntityId(), similarity));
							} else {
								n2.addNeighbor(new Tuple2<Integer, Double>(n1.getEntityId(), similarity));
							}
						}
						
					}
				}
			}
			
			for (MetablockingNode node : entitiesToCompare) {
				if (node.isSource()) {
					output.add(new Tuple2<Integer, MetablockingNode>(node.getEntityId(), node));
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
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(MetablockingNode.class)));
    
    
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, MetablockingNode>> graph = similarities.groupByKey(new MapFunction<Tuple2<Integer, MetablockingNode>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, MetablockingNode> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    //Execute the pruning removing the low edges (entities in the Neighbors list)
    Dataset<String> prunedGraph = graph.mapGroups(new MapGroupsFunction<Integer, Tuple2<Integer, MetablockingNode>, String>() {
    	
    	
    	@Override
		public String call(Integer key, Iterator<Tuple2<Integer, MetablockingNode>> values)
				throws Exception {
    		List<Tuple2<Integer, MetablockingNode>> nodes = IteratorUtils.toList(values);
			
    		MetablockingNode n1 = nodes.get(0)._2();//get the first node to merge with others.
			for (int j = 1; j < nodes.size(); j++) {
				MetablockingNode n2 = nodes.get(j)._2();
				n1.addSetNeighbors(n2);
			}
			
			n1.pruning();
			return n1.getEntityId() + "," + n1.toString();
		}


	}, Encoders.STRING());
    
    spark.streams().addListener(new StreamingQueryListener() {
		
    	@Override
        public void onQueryStarted(QueryStartedEvent queryStarted) {
            //System.out.println("Query started: " + queryStarted.id());
        }
        @Override
        public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
            //System.out.println("Query terminated: " + queryTerminated.id());
        }
        @Override
        public void onQueryProgress(QueryProgressEvent queryProgress) {
            System.out.println("Query duration (ms): " + queryProgress.progress().durationMs().get("triggerExecution"));
        }
	});
    		
    //Streaming query that stores the output incrementally.
    StreamingQuery query = prunedGraph
	  .selectExpr("CAST(value AS STRING)")
	  .writeStream()
	  .outputMode("update")
	  .format("kafka")
	  .option("kafka.bootstrap.servers", args[1])
	  .option("checkpointLocation", args[4])//C:\\Users\\lutibr\\Documents\\checkpoint\\
	  .option("topic", "outputtopic")
	  .trigger(Trigger.ProcessingTime(timeWindow + " seconds"))
	  .start();
    
    
    
    // Start running the query that prints the running counts to the console
	/*StreamingQuery query = prunedGraph.writeStream()
		.outputMode("update")
		.format("console")
		// .format("parquet") // can be "orc", "json", "csv", etc.
		// .option("checkpointLocation", "checkpoints/")
		// .option("path", OUTPUT_PATH)
		.trigger(Trigger.ProcessingTime(timeWindow + " seconds"))
		.start();*/
    
//    StreamingQuery query = prunedGraph.selectExpr("CAST(value AS STRING)").writeStream()
//    		.outputMode("update")
//			.format("kafka")
//			.option("kafka.bootstrap.servers", args[1])
//			.option("topic", "outputTopic")
//			.start();
    
//    StreamingQuery query = prunedGraph.writeStream()
//    		.outputMode("update")
//    		.foreach(new ForeachWriter<String>() {
//    			SingletonFileWriter writer;
//				
//				@Override
//				public void process(String line) {
//					//System.out.println(line);
//					try {
//						writer.save(line);
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//						System.out.println("Error to save the output file.");
//					}
//				}
//				
//				@Override
//				public boolean open(long elementID, long increment) {
//					writer = SingletonFileWriter.getInstance(OUTPUT_PATH, increment);
//					return true;
//				}
//				
//				@Override
//				public void close(Throwable arg0) {
//				}
//			})
//    		.trigger(Trigger.ProcessingTime(timeWindow + " seconds"))
//    		.start();
	

    query.awaitTermination();

    
  }

}
