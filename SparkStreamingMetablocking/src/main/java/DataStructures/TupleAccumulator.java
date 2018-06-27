package DataStructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.AccumulatorParam;

public class TupleAccumulator implements AccumulatorParam<Map<Integer, Node>>, Serializable {

    @Override
    public Map<Integer, Node> addAccumulator(Map<Integer, Node> t1, Map<Integer, Node> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<Integer, Node> addInPlace(Map<Integer, Node> r1, Map<Integer, Node> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<Integer, Node> zero(final Map<Integer, Node> initialValue) {
        return new HashMap<>();
    }

    private Map<Integer, Node> mergeMap( Map<Integer, Node> map1, Map<Integer, Node> map2) {
        Map<Integer, Node> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> b));
        return result;
    }

}