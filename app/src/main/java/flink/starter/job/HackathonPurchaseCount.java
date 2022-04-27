
package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Objects;
import java.util.UUID;

@Slf4j
public class HackathonPurchaseCount {

    private final String[] args;

    public HackathonPurchaseCount(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        HackathonPurchaseCount job = new HackathonPurchaseCount(args);
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ObjectMapper mapper = new ObjectMapper();

        /**
         * SOURCE
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.19.0.1:9092")
                .setTopics("bigin-shop")
                .setGroupId(UUID.randomUUID() + "")
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .setBounded(OffsetsInitializer.latest()) // 이거 기억하기!!!
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStream<ObjectNode> eventSource = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                        "KafkaSource")
                .map(event -> mapper.readValue(event, ObjectNode.class));

        eventSource
                .filter(event-> Objects.equals(event.get("name").asText(), "bg:purchase"))
                .addSink(new RichSinkFunction<ObjectNode>() {
            private int counter = 0;

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(counter);
            }

            @Override
            public void invoke(ObjectNode value) throws Exception {
                counter += 1;
            }
        });

        return env.execute("HackadayAnswer");
    }
}
