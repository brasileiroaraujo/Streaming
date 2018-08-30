package AccumulatorsAndBroadcasts;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class StreamingAccumulator {

	private static volatile LongAccumulator instance = null;

	public static LongAccumulator getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (StreamingAccumulator.class) {
				if (instance == null) {
					instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
				}
			}
		}
		return instance;
	}
}
