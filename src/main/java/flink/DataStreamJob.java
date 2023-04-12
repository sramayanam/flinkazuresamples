package flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InputStream input = DataStreamJob.class.getClassLoader().getResourceAsStream("consumer.properties");
        Properties properties = new Properties();
        properties.load(input);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(properties)
                .setTopics("click_events")
                .setGroupId("$Default")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");
        //stream.print();

       String outputPath  = "abfss://datastore@srramstore.dfs.core.windows.net/flink/click_events";
        final FileSink<String> sink =  FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(3))
                                .withMaxPartSize(MemorySize.ofMebiBytes(5))
                                .build())
                .build();

       stream.sinkTo(sink);

      // stream.writeAsText("wasbs://datastore@srramstore.blob.core.windows.net/test.txt");
        env.execute();
    }
}
