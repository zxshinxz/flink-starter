package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class StreamJob {

    private final String[] args;

    public StreamJob(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        StreamJob job = new StreamJob(args);
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new GzipSource("S9ETP3E0TF-5_2022_04_03.gz"))
                .map(input -> 1)
                .addSink(new CounterSink());

        return env.execute("StreamJob");
    }
}
