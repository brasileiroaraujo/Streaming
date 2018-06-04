package DataStructures;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

import scala.Serializable;
import scala.Tuple2;

public class MapAccumulator implements AccumulatorParam<Map<Integer, Tuple2<Double, Double>>>, Serializable {

    @Override
    public Map<Integer, Tuple2<Double, Double>> addAccumulator(Map<Integer, Tuple2<Double, Double>> t1, Map<Integer, Tuple2<Double, Double>> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<Integer, Tuple2<Double, Double>> addInPlace(Map<Integer, Tuple2<Double, Double>> r1, Map<Integer, Tuple2<Double, Double>> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<Integer, Tuple2<Double, Double>> zero(final Map<Integer, Tuple2<Double, Double>> initialValue) {
        return new HashMap<>();
    }

    private Map<Integer, Tuple2<Double, Double>> mergeMap( Map<Integer, Tuple2<Double, Double>> map1, Map<Integer, Tuple2<Double, Double>> map2) {
        Map<Integer, Tuple2<Double, Double>> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> new Tuple2<Double, Double>(Math.max(a._1(), b._1()), Math.max(a._2(), b._2()))));

        return result;
    }

}
