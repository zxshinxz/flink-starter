
package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

@Slf4j
public class SessionCounter {

    private final String[] args;

    public SessionCounter(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        SessionCounter job = new SessionCounter(args);
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ObjectMapper mapper = new ObjectMapper();

        DataStream<String> eventString = env.addSource(new GzipSource("S9ETP3E0TF-5_2022_04_03.gz"));

        DataStream<ObjectNode> eventObjectNode = eventString.map(event -> mapper.readValue(event, ObjectNode.class));

        DataStream<ObjectNode> eventObjectNodeWithWatermark = eventObjectNode.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ObjectNode>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(0L);
            }

            @Override
            public long extractTimestamp(ObjectNode element, long recordTimestamp) {
                return element.get("timestamp").asLong();
            }
        });

        eventObjectNodeWithWatermark.keyBy(event -> event.get("device").asText())
                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
                .process(new ProcessWindowFunction<ObjectNode, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<ObjectNode, Integer, String, TimeWindow>.Context context, Iterable<ObjectNode> elements, Collector<Integer> out) throws Exception {
                        out.collect(1);
                    }
                })
                .addSink(new RichSinkFunction<Integer>() {
                    private int counter = 0;

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println(counter);
                    }

                    @Override
                    public void invoke(Integer value) throws Exception {
                        counter += value;
                    }
                });

        return env.execute("StreamJob");
    }
}
