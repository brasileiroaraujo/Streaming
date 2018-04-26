package streaming.util;

import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class JavaWordBlacklist {

	  private static volatile Broadcast<HashSet<String>> instance = null;

	  public static Broadcast<HashSet<String>> getInstance(JavaSparkContext jsc, HashSet<String> wordBlacklist) {
	    if (instance == null) {
	      synchronized (JavaWordBlacklist.class) {
	        if (instance == null) {
	          instance = jsc.broadcast(wordBlacklist);
	        }
	      }
	    }
	    return instance;
	  }
	}


