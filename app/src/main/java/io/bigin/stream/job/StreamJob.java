package io.bigin.stream.job;

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

    // TODO: need to implement this
    public JobExecutionResult execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("test")
                .print();

        return env.execute("StreamJob");
    }
}
