package PrimeBigdata;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.State;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.common.collect.Lists;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeCollection;
import DataStructures.SingletonFileWriter;
import DataStructures.StringCollection;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.BoxedUnit;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PRIMEStructuredWindowedBigdata {
  

public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = args[3];  //$$ will be replaced by the increment index //"outputs/teste.txt";
	  int timeWindow = Integer.parseInt(args[0]); //We have configured the period to x seconds (x sec).
	  int numNodes = Integer.parseInt(args[5]);
	  
    
    SparkSession spark = SparkSession
    		  .builder()
    		  .appName("PRIMEStructuredWatermarkTimeout")
    		  .master("local[1]")
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
    
    Dataset<Tuple2<Integer, String>> streamOfPairs = entities.flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, String>>() {

		@Override
		public Iterator call(EntityProfile se) throws Exception {
			List<Tuple2<Integer, String>> output = new ArrayList<Tuple2<Integer, String>>();
			
			Set<Integer> cleanTokens = new HashSet<Integer>();
			
			for (Attribute att : se.getAttributes()) {
//				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
				KeywordGenerator kw = new KeywordGeneratorImpl();
				for (String string : kw.generateKeyWords(att.getValue())) {
					cleanTokens.add(string.hashCode());
				}
			}
			
			String node = se.getKey() + ";" + se.isSource() + ";" + cleanTokens.toString().replaceAll("\\[|\\]|\\s", "");
			
			for (Integer tk : cleanTokens) {
				output.add(new Tuple2<Integer, String>(tk, node));
			}
			
			return output.iterator();
		}
	}, Encoders.tuple(Encoders.INT(), Encoders.STRING())).repartition(numNodes);
    
    
	//define the blocks based on the tokens <tk, [n1,n2,n3]>, where n is a node.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, String>> entityBlocks = streamOfPairs.groupByKey(new MapFunction<Tuple2<Integer, String>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, String> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    //State function
    MapGroupsWithStateFunction<Integer, Tuple2<Integer, String>, StringCollection, Tuple2<Integer, StringCollection>> stateUpdateFunc =
    	      new MapGroupsWithStateFunction<Integer, Tuple2<Integer, String>, StringCollection, Tuple2<Integer, StringCollection>>() {

				@Override
				public Tuple2<Integer, StringCollection> call(Integer key, Iterator<Tuple2<Integer, String>> values, GroupState<StringCollection> state)
						throws Exception {
					StringCollection count = (state.exists() ? state.get() : new StringCollection());
					if (state.hasTimedOut()) {
						state.remove();
						Tuple2<Integer, StringCollection> thisOne = new Tuple2<>(key, new StringCollection());
						return thisOne;
					} else {
						List<Tuple2<Integer, String>> listOfBlocks = IteratorUtils.toList(values);
						
//						count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds
						
						count.setAsMarked("F");
//						for (String entBlocks : count.getNodeList()) {
//							entBlocks.setMarked(false);
//						}

						for (Tuple2<Integer, String> entBlocks : listOfBlocks) {
							String content = entBlocks._2();
							if (content.split(";").length == 3) {
								content += ";T";
							} else {
								content = content.substring(0, content.length()-2) + ";T";
							}
							count.add(content);
						}

						Tuple2<Integer, StringCollection> thisOne = new Tuple2<>(key, count);
						state.update(count);
						
						state.setTimeoutDuration(args[2] + " seconds");

						return thisOne;
					}
					
				}
    	      };
	
    //Save the entity blocks on State to be used in the nexts increments. 
    Dataset<Tuple2<Integer, StringCollection>> storedBlocks = entityBlocks.mapGroupsWithState(
            stateUpdateFunc,
            Encoders.javaSerialization(StringCollection.class),
            Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(StringCollection.class)),
            GroupStateTimeout.ProcessingTimeTimeout());//ESSA LINHA PODE SER AVALIADA DEPOIS
    
    Dataset<Tuple2<Integer, StringCollection>> storedBlocksPart = storedBlocks.repartition(numNodes);
    
    //DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    Dataset<Tuple2<Integer, String>> pairEntityBlock = storedBlocksPart.flatMap(new FlatMapFunction<Tuple2<Integer, StringCollection>, Tuple2<Integer, String>>() {
		@Override
		public Iterator<Tuple2<Integer, String>> call(Tuple2<Integer, StringCollection> t) throws Exception {
			List<String> entitiesToCompare = t._2().getNodeList();
			List<Tuple2<Integer, String>> output = new ArrayList<Tuple2<Integer, String>>();
			for (int i = 0; i < entitiesToCompare.size(); i++) {
				String[] n1 = entitiesToCompare.get(i).split(";");
				for (int j = i+1; j < entitiesToCompare.size(); j++) {
					String[] n2 = entitiesToCompare.get(j).split(";");
					//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
					if (!n1[1].equalsIgnoreCase(n2[1]) && (n1[3].equalsIgnoreCase("T") || n2[3].equalsIgnoreCase("T"))) {
						double similarity = calculateSimilarity(t._1(), new HashSet<String>(Arrays.asList(n1[2].split(","))), new HashSet<String>(Arrays.asList(n2[2].split(","))));
						if (similarity >= 0) {
//							numberOfComparisons.add(1);
							if (n1[1].equalsIgnoreCase("true")) {
								if (entitiesToCompare.get(i).split(";").length == 5) {
									entitiesToCompare.set(i, entitiesToCompare.get(i) + n2[0] + ":" +  similarity + "-");
								} else {
									entitiesToCompare.set(i, entitiesToCompare.get(i) + ";" + n2[0] + ":" +  similarity + "-");
								}
								//n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
							} else {
								if (entitiesToCompare.get(j).split(";").length == 5) {
									entitiesToCompare.set(j, entitiesToCompare.get(j) + n1[0] + ":" +  similarity + "-");
								} else {
									entitiesToCompare.set(j, entitiesToCompare.get(j) + ";" + n1[0] + ":" +  similarity + "-");
								}
//								n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
							}
						}
						
					}
				}
			}
			
			for (String node : entitiesToCompare) {
				if (node.split(";").length == 5 && node.split(";")[1].equalsIgnoreCase("true")) {
					output.add(new Tuple2<Integer, String>(Integer.parseInt(node.split(";")[0]), node));
				}
			}
			return output.iterator();
		}

		private double calculateSimilarity(Integer blockKey, Set<String> ent1, Set<String> ent2) {
			int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
			Set<String> intersect = new HashSet<String>(ent1);
			intersect.retainAll(ent2);

			// MACOBI strategy
			if (!min(intersect).equals(blockKey)) {
				return -1;
			}

			if (maxSize > 0) {
				double x = (double) intersect.size() / maxSize;
				return x;
			} else {
				return 0;
			}
		}

		private Integer min(Set<String> intersect) {
			int min = Integer.MAX_VALUE;
			for (String v : intersect) {
				if (Integer.parseInt(v) < min) {
					min = Integer.parseInt(v);
				}
			}
			return min;
		}
	}, Encoders.tuple(Encoders.INT(), Encoders.STRING())).repartition(numNodes);
    
    
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    KeyValueGroupedDataset<Integer, Tuple2<Integer, String>> graph = pairEntityBlock.groupByKey(new MapFunction<Tuple2<Integer, String>, Integer>() {

		@Override
		public Integer call(Tuple2<Integer, String> n) throws Exception {
			return n._1();
		}
	}, Encoders.INT());
    
    
    //Execute the pruning removing the low edges (entities in the Neighbors list)
    Dataset<String> prunedGraph = graph.mapGroups(new MapGroupsFunction<Integer, Tuple2<Integer, String>, String>() {
    	
    	
    	@Override
		public String call(Integer key, Iterator<Tuple2<Integer, String>> values)
				throws Exception {
    		List<Tuple2<Integer, String>> nodes = IteratorUtils.toList(values);
    		
    		
    		double sumWeight = 0;
    		double numberOfNeighbors = 0;
			for (Tuple2<Integer, String> pair : nodes) {
    			String[] neighbors = pair._2().split(";")[4].split("-");
    			numberOfNeighbors += neighbors.length;
    			for (String similarity : neighbors) {
    				if (similarity.isEmpty()) {
    					numberOfNeighbors--;
					} else {
						sumWeight += Double.parseDouble(similarity.split(":")[1]);
					}
					
				}
			}   		
    		
    		double threshold = sumWeight/numberOfNeighbors;
    		Set<Tuple2<Integer, Double>> prunnedNeighbors = new HashSet<>();
    		
    		
    		for (Tuple2<Integer, String> pair : nodes) {
    			String[] neighbors = pair._2().split(";")[4].split("-");
    			for (String similarity : neighbors) {
    				if (!similarity.isEmpty()) {
    					String[] output = similarity.split(":");
    					if (Double.parseDouble(output[1]) >= threshold) {
    						prunnedNeighbors.add(new Tuple2<Integer, Double>(Integer.parseInt(output[0]), Double.parseDouble(output[1])));
    					}
    					
					}
					
				}
			} 
    		
			return key + "," + prunnedNeighbors.toString();
		}


	}, Encoders.STRING()).repartition(numNodes);
    
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
            System.out.println(queryProgress.progress().durationMs().get("triggerExecution"));//Query duration (ms)
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
