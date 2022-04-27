
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

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class HackathonTopProduct {

    private final String[] args;

    public HackathonTopProduct(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        HackathonTopProduct job = new HackathonTopProduct(args);
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
                .filter(event -> Objects.equals(event.get("name").asText(), "bg:itemPurchase"))
                .addSink(new RichSinkFunction<ObjectNode>() {
                    private Map<String, Integer> counter = new HashMap<>();

                    @Override
                    public void close() throws Exception {
                        super.close();
                        List<Map.Entry<String, Integer>> products = counter.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getValue)).collect(Collectors.toList());
                        Map.Entry<String, Integer> top = products.get(products.size() - 1);
                        System.out.println(top);
                    }

                    @Override
                    public void invoke(ObjectNode value) throws Exception {
                        if (value.has("event_seg_purchaseId")) {
                            String id = value.get("event_seg_purchaseId").asText();
                            Integer quantity = value.get("event_seg_productQuantity").asInt();
                            counter.put(id, counter.getOrDefault(id, 0) + quantity);
                        }
                    }
                });

        return env.execute("HackadayAnswer");
    }
}
