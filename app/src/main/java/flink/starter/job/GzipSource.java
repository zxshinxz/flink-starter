package flink.starter.job;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class GzipSource implements SourceFunction<String> {
    private List<String> files;

    public GzipSource(List<String> files) {
        this.files = files;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        this.files.forEach(path -> {
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(Paths.get(path))), StandardCharsets.UTF_8))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    ctx.collect(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void cancel() {

    }
}
