package streaming.util;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;

public class BroadcastCollectionTokenKeys {

	private static volatile Broadcast<List<String>> instance = null;

	public static Broadcast<List<String>> getInstance(JavaSparkContext jsc, CollectionAccumulator<String> accum) {
		if (instance == null) {
			synchronized (BroadcastCollectionTokenKeys.class) {
				if (instance == null) {
//					List<String> wordBlacklist = Arrays.asList("a", "b", "c");
					instance = jsc.broadcast(accum.value());
				}
			}
		}
		return instance;
	}
	
	public static void clean() {
		instance = null;
	}
}
