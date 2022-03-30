package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        // Source
        DataStream<String> source = env.fromElements("test1", "test2")
                .name("source-from-element")
                .uid("source-from-element");

        // Sink
        source.print()
                .name("sink-print-console")
                .uid("sink-print-console");

        return env.execute("StreamJob");
    }
}
