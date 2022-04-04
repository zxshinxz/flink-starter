package flink.starter.job;


import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CounterSink extends RichSinkFunction<Integer> {

    private int counter;

    public CounterSink() {
        this.counter = 0;
    }

    @Override
    public void close() throws Exception {
        super.close();

        System.out.println(counter);
    }

    @Override
    public void invoke(Integer value, Context context) throws Exception {
        super.invoke(value, context);
        this.counter += value;
    }
}
