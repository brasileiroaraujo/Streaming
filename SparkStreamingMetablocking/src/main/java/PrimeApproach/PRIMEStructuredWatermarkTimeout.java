package PrimeApproach;

import java.io.IOException;
import java.sql.Timestamp;
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
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.runtime.BoxedUnit;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PRIMEStructuredWatermarkTimeout {
  

public static void main(String[] args) throws InterruptedException, StreamingQueryException {
//	  System.setProperty("hadoop.home.dir", "K:/winutils/");
	  String OUTPUT_PATH = args[3];  //$$ will be replaced by the increment index //"outputs/teste.txt";
	  int timeWindow = Integer.parseInt(args[0]); //We have configured the period to x seconds (x sec).
	  int numNodes = Integer.parseInt(args[5]);
	  
    
    SparkSession spark = SparkSession
    		  .builder()
    		  .appName("PRIMEStructuredWatermarkTimeout")
//    		  .master("local[6]")
    		  .getOrCreate();
    
    Dataset<String> lines = spark
    		  .readStream()
    		  .format("kafka")
    		  .option("kafka.bootstrap.servers", args[1])//localhost:9092    10.171.171.50:8088
    		  .option("subscribe", "mytopic")
//    		  .option("failOnDataLoss", "false")
    		  .load()
    		  .selectExpr("CAST(value AS STRING)")
    	      .as(Encoders.STRING());
    
    
    StructType structType = new StructType();
    structType = structType.add("isSource", DataTypes.BooleanType, false);
    structType = structType.add("key", DataTypes.IntegerType, false);
    structType = structType.add("attValues", DataTypes.StringType, false);
    structType = structType.add("creation", DataTypes.TimestampType, false);

    ExpressionEncoder<Row> encoderRow = RowEncoder.apply(structType);
    
    Dataset<Row> entities = lines.flatMap(new FlatMapFunction<String, Row>() {
        @Override
        public Iterator<Row> call(String s) throws Exception {
        	String[] attributes = s.split("<<>>");
            // a static map operation to demonstrate
            List<Object> data = new ArrayList<>();
            data.add(Boolean.parseBoolean(attributes[0]));
            data.add(Integer.parseInt(attributes[1]));
            if (attributes.length == 3) {
            	data.add(attributes[2]);
			} else {
				data.add(" ");
			}
            data.add(new Timestamp(System.currentTimeMillis()));
            ArrayList<Row> list = new ArrayList<>();
            list.add(RowFactory.create(data.toArray()));
            return list.iterator();
        }
    }, encoderRow).withWatermark("creation", args[2] + " seconds");
    
    
    // Generate running word count
//    Dataset<EntityProfile> entities = entitiesMarked.map(r -> new EntityProfile(r.getString(0)), Encoders.bean(EntityProfile.class));
    
	//INIT TIME
    long initTime = System.currentTimeMillis();
    LongAccumulator timeForEachInteration = spark.sparkContext().longAccumulator();
    timeForEachInteration.add(initTime);
//	DoubleAccumulator sumTimePerInterations = spark.sparkContext().doubleAccumulator();
//	DoubleAccumulator numberInterations = spark.sparkContext().doubleAccumulator();
    
    Dataset<Tuple2<Integer, Node>> streamOfPairs = entities.flatMap(new FlatMapFunction<Row, Tuple2<Integer, Node>>() {

		@Override
		public Iterator call(Row se) throws Exception {
			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();
			
			Set<Integer> cleanTokens = new HashSet<Integer>();
			
			boolean isSource = se.getBoolean(0);
			int key = se.getInt(1);
			String attValues = se.getString(2);
			
			KeywordGenerator kw = new KeywordGeneratorImpl();
			for (String string : kw.generateKeyWords(attValues)) {
				cleanTokens.add(string.hashCode());
			}
			
//			for (Attribute att : se.getAttributes()) {
////				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
//				KeywordGenerator kw = new KeywordGeneratorImpl();
//				for (String string : kw.generateKeyWords(att.getValue())) {
//					cleanTokens.add(string.hashCode());
//				}
//			}
			
			Node node = new Node(key, cleanTokens, new HashSet<>(), isSource);
			
			for (Integer tk : cleanTokens) {
				node.setTokenTemporary(tk);
				output.add(new Tuple2<Integer, Node>(tk, node));
			}
			
			return output.iterator();
		}
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class))).repartition(numNodes);
    
    
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
					if (state.hasTimedOut()) {
						state.remove();
						Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);
						return thisOne;
					} else {
						List<Tuple2<Integer, Node>> listOfBlocks = IteratorUtils.toList(values);
						
						count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds
						
						for (Node entBlocks : count.getNodeList()) {
							entBlocks.setMarked(false);
						}

						for (Tuple2<Integer, Node> entBlocks : listOfBlocks) {
							entBlocks._2().setMarked(true);
							count.add(entBlocks._2());
						}

						Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);
						state.update(count);
						
						state.setTimeoutDuration(args[2] + " seconds");

						return thisOne;
					}
					
				}
    	      };
	
    //Save the entity blocks on State to be used in the nexts increments. 
    Dataset<Tuple2<Integer, NodeCollection>> storedBlocks = entityBlocks.mapGroupsWithState(
            stateUpdateFunc,
            Encoders.javaSerialization(NodeCollection.class),
            Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(NodeCollection.class)),
            GroupStateTimeout.ProcessingTimeTimeout());//ESSA LINHA PODE SER AVALIADA DEPOIS
    
    Dataset<Tuple2<Integer, NodeCollection>> storedBlocksPart = storedBlocks.repartition(numNodes);
    
    //DoubleAccumulator numberOfComparisons = spark.sparkContext().doubleAccumulator();
    
    //The comparison between the entities (i.e., the Nodes) are performed.
    Dataset<Tuple2<Integer, Node>> pairEntityBlock = storedBlocksPart.flatMap(new FlatMapFunction<Tuple2<Integer, NodeCollection>, Tuple2<Integer, Node>>() {
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
//							numberOfComparisons.add(1);
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
	}, Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(Node.class))).repartition(numNodes);
    
    
    
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
