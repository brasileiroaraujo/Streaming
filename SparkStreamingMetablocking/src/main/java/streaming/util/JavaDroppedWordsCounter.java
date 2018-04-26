package streaming.util;

import java.util.HashSet;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaDroppedWordsCounter {

	private static volatile Accumulator<HashSet<String>> instance = null;

	public static Accumulator<HashSet<String>> getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (JavaDroppedWordsCounter.class) {
				if (instance == null) {
					instance = jsc.sc().accumulator(new HashSet<String>(), new AccumulatorParamSet());
				}
			}
		}
		return instance;
	}
}
