package flink.starter.job;

import org.apache.flink.api.common.functions.ReduceFunction;

public class Reduce implements ReduceFunction<Integer> {

    @Override
    public Integer reduce(Integer value1, Integer value2) throws Exception {
        return value1 + value2;
    }
}
