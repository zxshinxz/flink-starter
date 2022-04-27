/*
 * Created by Andy Shin on 2022-04-24
 * As part of Bigin
 *
 * Copyright (C) Bigin (https://bigin.io/main) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Dev Unit Team <dragon@bigin.io>, 2022-04-24
 */

package flink.starter.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * create on 2022-04-24.
 * create by Bigin.
 *
 * <p> 클래스 설명 </p>
 *
 * @author Andy Shin
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class DataInsertJob {
    private final String[] args;

    public DataInsertJob(String[] args) {
        this.args = args;
    }


    public static void main(String[] args) throws Exception {
        DataInsertJob job = new DataInsertJob(args);
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> files = new ArrayList<>();
//        files.add("./data/S9ETP3E0TF-5_2022_04_01.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_02.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_03.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_04.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_05.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_06.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_07.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_08.gz");
//        files.add("./data/S9ETP3E0TF-5_2022_04_09.gz");
        files.add("./data/S9ETP3E0TF-5_2022_04_10.gz");
        DataStream<String> eventString = env.addSource(new GzipSource(files));


        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(properties)
                .setBootstrapServers("192.168.0.195:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bigin-shop")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        eventString.sinkTo(sink);

        return env.execute("StreamJob");
    }
}
