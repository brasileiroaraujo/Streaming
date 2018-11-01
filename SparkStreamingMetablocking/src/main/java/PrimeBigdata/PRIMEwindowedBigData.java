package PrimeBigdata;

import java.io.IOException;
import java.sql.Timestamp;
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

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import com.google.common.collect.Lists;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeCollection;
import DataStructures.SingletonFileWriter;
import kafka.serializer.StringDecoder;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.BoxedUnit;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PRIMEwindowedBigData {
  

public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = args[3];  //$$ will be replaced by the increment index //"outputs/teste.txt";
	  int timeWindow = Integer.parseInt(args[0]); //We have configured the period to x seconds (x sec).
	  int windowThreshold = Integer.parseInt(args[2]);
	  int numNodes = Integer.parseInt(args[5]);
	  
    
//	SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PRIMEwindowedBigData");
	SparkSession spark = SparkSession
	        .builder()
	        .appName("PRIMEwindowedBigData")
	        .master("local[4]")
	        .getOrCreate();
	
	JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
//	JavaStreamingContext jssc = new JavaStreamingContext(spark.sparkContext(), Durations.seconds(timeWindow));
	 JavaStreamingContext ssc = new JavaStreamingContext(jssc, Durations.seconds(timeWindow));
	 ssc.checkpoint(args[4]);
	
	
	//kafka pool to receive streaming data
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", args[1]);
    Set<String> topics = Collections.singleton("mytopic");

    JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(ssc,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
    

	//INIT TIME
//    long initTime = System.currentTimeMillis();
//    LongAccumulator timeForEachInteration = spark.sparkContext().longAccumulator();
//    timeForEachInteration.add(initTime);
//	DoubleAccumulator sumTimePerInterations = spark.sparkContext().doubleAccumulator();
//	DoubleAccumulator numberInterations = spark.sparkContext().doubleAccumulator();
    
    //INIT TIME
    long initTime = System.currentTimeMillis();
    Accumulator<Double> timeForEachInteration = jssc.accumulator(initTime);
    Accumulator<Double> sumTimePerInterations = jssc.accumulator(0.0);
    Accumulator<Integer> numberInterations = jssc.accumulator(0);
    
    JavaDStream<EntityProfile> entities = lines.map(s -> new EntityProfile(s._2()));
    
    JavaPairDStream<Integer, Node> streamOfPairs = entities.flatMapToPair(new PairFlatMapFunction<EntityProfile, Integer, Node>() {

		@Override
		public Iterator<Tuple2<Integer, Node>> call(EntityProfile se) throws Exception {
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
	});
    
    
	//define the blocks based on the tokens <tk, [n1,n2,n3]>, where n is a node.
    JavaPairDStream<Integer, Iterable<Node>> entityBlocks = streamOfPairs.groupByKey().repartition(numNodes);
    
    
    //State function
   	Function4<Time, Integer, Optional<Iterable<Node>>, State<NodeCollection>, Optional<Tuple2<Integer, NodeCollection>>> stateUpdateFunc = (
    	  			time, key, values, state) -> {
  				NodeCollection count = (state.exists() ? state.get() : new NodeCollection());
				if (state.isTimingOut()) {
					state.remove();
					Tuple2<Integer, NodeCollection> thisOne = new Tuple2<Integer, NodeCollection>(key, count);
					return Optional.of(thisOne);
				} else {
					List<Node> listOfBlocks = StreamSupport.stream(values.get().spliterator(), false)
	    	  				.collect(Collectors.toList());
					
//					count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds
					
					for (Node entBlocks : count.getNodeList()) {
						entBlocks.setMarked(false);
					}

					for (Node entBlocks : listOfBlocks) {
						entBlocks.setMarked(true);
						count.add(entBlocks);
					}

					Tuple2<Integer, NodeCollection> thisOne = new Tuple2<Integer, NodeCollection>(key, count);
					state.update(count);
					
//					state.setTimeoutDuration(args[2] + " seconds");
					time.plus(Durations.seconds(windowThreshold));

					return Optional.of(thisOne);
				}
    	  	};
	
    //Save the entity blocks on State to be used in the nexts increments. 
    JavaDStream<Tuple2<Integer, NodeCollection>> storedBlocks = entityBlocks
    	  			.mapWithState(StateSpec.function(stateUpdateFunc)).repartition(numNodes);
    
//    Dataset<Tuple2<Integer, NodeCollection>> storedBlocksPart = storedBlocks.repartition(numNodes);
    
    //DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    Accumulator<Integer> numberOfComparisons = jssc.accumulator(0);
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    JavaPairDStream<Integer, Node> pairEntityBlock = storedBlocks.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,NodeCollection>, Integer, Node>(){

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
    	
    });
    		
    	
    
    
    //A beginning block is generated, notice that we have all the Neighbors of each entity.
    JavaPairDStream<Integer, Iterable<Node>> graph = pairEntityBlock.groupByKey().repartition(numNodes);    
    
    //Execute the pruning removing the low edges (entities in the Neighbors list)
    JavaDStream<String> prunedGraph = graph.map(new Function<Tuple2<Integer,Iterable<Node>>, String>() {

		@Override
		public String call(Tuple2<Integer, Iterable<Node>> values) throws Exception {
			List<Node> nodes = StreamSupport.stream(values._2().spliterator(), false)
				.collect(Collectors.toList());
			
			Node n1 = nodes.get(0);//get the first node to merge with others.
			for (int j = 1; j < nodes.size(); j++) {
				Node n2 = nodes.get(j);
				n1.addSetNeighbors(n2);
			}
			
			n1.pruning();
			return n1.getId() + "," + n1.toString();
		}
	});
    
    prunedGraph.foreachRDD(rdd ->{
    	//	System.err.println("Chegou no Fim: " + ((System.currentTimeMillis() - initTime)/1000));
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
//    		    	for (String string : rdd.collect()) {
//						System.out.println(string);
//					}
//    		       rdd.saveAsTextFile(OUTPUT_PATH);
    		    }
    	});
        
        
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();
    ssc.awaitTermination();
    System.out.println("*** Streaming terminated");
    
    
    
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
	

    
  }

}
