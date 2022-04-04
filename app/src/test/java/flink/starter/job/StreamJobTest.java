package flink.starter.job;

import flink.starter.util.TestSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StreamJobTest {
    private static Logger LOGGER = null;
    private static final int PARALLELISM = 1;

    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    @DisplayName("should have 2 events")
    void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> source = env.fromElements("test1", "test2");

        source.addSink(new TestSink<>("result"));
        JobExecutionResult jobResult = env.execute();

        Map<String, Object> accumMap = jobResult.getAllAccumulatorResults();
        List<String> output = (List<String>) accumMap.get("result");

        assertThat(output.size()).isEqualTo(2);
        assertThat(output.get(0)).isEqualTo("test1");
        assertThat(output.get(1)).isEqualTo("test2");
    }
}
