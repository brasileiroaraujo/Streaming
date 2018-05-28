package KafkaIntegration;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaSparkStructured {
	public static void main(String[] args) throws StreamingQueryException {
		String bootstrapServers = "localhost:9092";
	    String subscribeType = "subscribe";
	    String topics = "mytopic";

	    SparkSession spark = SparkSession
	      .builder()
	      .appName("KafkaSparkStructured")
	      .master("local[1]")
	      .getOrCreate();

	    // Create DataSet representing the stream of input lines from kafka
	    Dataset<String> lines = spark
	      .readStream()
	      .format("kafka")
	      .option("kafka.bootstrap.servers", bootstrapServers)
	      .option(subscribeType, topics)
	      .load()
	      .selectExpr("CAST(value AS STRING)")
	      .as(Encoders.STRING());

	    // Generate running word count
	    Dataset<Row> wordCounts = lines.flatMap(new FlatMapFunction() {

			@Override
			public Iterator call(Object t) throws Exception {
				return Arrays.asList(((String) t).split(" ")).iterator();
			}
		}, Encoders.STRING()
	        ).groupBy("value").count();
	    
//	    (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
//        Encoders.STRING()).groupBy("value").count()
	    
	    
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
		  .outputMode("complete")
		  .format("console")
		  .start();
	    
	    query.awaitTermination();
	}
}
