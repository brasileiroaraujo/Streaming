package streaming.util;

import java.util.HashSet;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;

public class AccumulatorCollectionTokenKey {

	private static volatile CollectionAccumulator<String> instance = null;

	public static CollectionAccumulator<String> getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (AccumulatorCollectionTokenKey.class) {
				if (instance == null) {
					instance = jsc.sc().collectionAccumulator("accCollection");
				}
			}
		}
		return instance;
	}
}

