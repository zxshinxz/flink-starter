package flink.starter.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class NFlatMap implements FlatMapFunction<Integer, Integer> {
    @Override
    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
        for (int i = 0; i < value; i++)
            out.collect(value);
    }
}
