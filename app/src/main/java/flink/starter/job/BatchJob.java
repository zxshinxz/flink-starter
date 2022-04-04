package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.ExecutionEnvironment;

@Slf4j
public class BatchJob {

    private final String[] args;

    public BatchJob(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Long result = env.readTextFile("S9ETP3E0TF-5_2022_04_03.gz")
                .count();

        System.out.println(result);
    }
}
