package streaming.util;

import java.util.HashSet;

import org.apache.spark.AccumulatorParam;

public class AccumulatorParamSet implements AccumulatorParam {

	@Override
	public Object addInPlace(Object arg0, Object arg1) {
		HashSet<String> set1 = (HashSet<String>) arg0;
		HashSet<String> set2 = (HashSet<String>) arg1;
		set1.addAll(set2);
		return set1;
	}

	@Override
	public Object zero(Object arg0) {
		return new HashSet<String>();
	}

	@Override
	public Object addAccumulator(Object arg0, Object arg1) {
		HashSet<String> set1 = (HashSet<String>) arg0;
		HashSet<String> set2 = (HashSet<String>) arg1;
		set1.addAll(set2);
		return set1;
	}

}
