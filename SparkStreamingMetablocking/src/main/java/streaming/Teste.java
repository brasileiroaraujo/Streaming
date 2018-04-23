package streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.apache.spark.Accumulator;
import org.apache.spark.ContextCleaner;
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
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import scala.Tuple2;
import streaming.util.AccumulatorParamSet;
import streaming.util.CSVFileStreamGeneratorPMSD;


//Parallel-based Metablockig for Streaming Data
public class Teste {
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
    
//    ContextCleaner cleaner = new ContextCleaner(spark.sparkContext());

    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    

    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));
    
    CollectionAccumulator<String> accum = sc.sc().collectionAccumulator();
    
    sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(""+x));
    
    Broadcast<List<String>> broadcastVar = sc.broadcast(accum.value());
    
    
    sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(new VoidFunction<Integer>() {

		@Override
		public void call(Integer x) throws Exception {
			List<String> list = broadcastVar.value();
			if (list.contains(x+"")) {
				System.out.println(x);
			}
		}
	});
    

    
    

  }

}

