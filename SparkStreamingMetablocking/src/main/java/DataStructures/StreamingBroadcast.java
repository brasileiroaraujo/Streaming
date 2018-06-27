package DataStructures;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class StreamingBroadcast {

	  private static volatile Broadcast<Map<Integer, Node>> instance = null;

	  public static Broadcast<Map<Integer, Node>> getInstance(JavaSparkContext jsc, Map<Integer, Node> map) {
	    if (instance == null) {
	      synchronized (StreamingBroadcast.class) {
	        if (instance == null) {
	          instance = jsc.broadcast(map);
	        }
	      }
	    }
	    return instance;
	  }
}
