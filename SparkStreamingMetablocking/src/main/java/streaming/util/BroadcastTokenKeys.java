package streaming.util;

import java.util.HashSet;

import org.apache.spark.Accumulator;
import org.apache.spark.CleanAccum;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastTokenKeys {

	private static volatile Broadcast<HashSet<String>> instance = null;

	public static Broadcast<HashSet<String>> getInstance(JavaSparkContext jsc, Accumulator<HashSet<String>> accum) {
		if (instance == null) {
			synchronized (BroadcastTokenKeys.class) {
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
