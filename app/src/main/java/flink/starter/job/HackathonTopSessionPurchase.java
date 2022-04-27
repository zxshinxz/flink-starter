
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class HackathonTopSessionPurchase {

    private final String[] args;

    public HackathonTopSessionPurchase(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        HackathonTopSessionPurchase job = new HackathonTopSessionPurchase(args);
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

        DataStream<ObjectNode> streamWithTimestamp = eventSource.assignTimestampsAndWatermarks(WatermarkStrategy.<ObjectNode>forBoundedOutOfOrderness(Duration.ofMinutes(1000000))
                .withTimestampAssigner((event, timestamp) -> event.get("timestamp").asLong()));


        streamWithTimestamp.keyBy(event -> event.get("device").asText())
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .process(new ProcessWindowFunction<ObjectNode, List<ObjectNode>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<ObjectNode, List<ObjectNode>, String, TimeWindow>.Context context, Iterable<ObjectNode> elements, Collector<List<ObjectNode>> out) throws Exception {
                        List<ObjectNode> sortedSession = StreamSupport.stream(Spliterators.spliteratorUnknownSize(elements.iterator(), Spliterator.ORDERED), false)
                                .filter(event -> Objects.equals(event.get("name").asText(), "bg:purchase")).collect(Collectors.toList());
                        out.collect(sortedSession);
                    }

                })
                .addSink(new RichSinkFunction<List<ObjectNode>>() {
                    private Integer max = 0;

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println(max);
                    }

                    @Override
                    public void invoke(List<ObjectNode> session) throws Exception {
                        if (this.max < session.stream().map(event -> event.get("metric_revenue").asInt()).reduce(0, Integer::sum))
                            System.out.println("test");
                        this.max = Math.max(this.max, session.stream().map(event -> event.get("metric_revenue").asInt()).reduce(0, Integer::sum));
                    }
                });


        return env.execute("HackadayAnswer");
    }
}
