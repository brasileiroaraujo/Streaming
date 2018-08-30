package AccumulatorsAndBroadcasts;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class StreamingBroadcast {
	private static volatile Broadcast<Long> instance = null;

	public static Broadcast<Long> getInstance(JavaSparkContext jsc, Long value) {
		if (instance == null) {
			synchronized (StreamingBroadcast.class) {
				if (instance == null) {
					instance = jsc.broadcast(value);
				}
			}
		}
		return instance;
	}
}
