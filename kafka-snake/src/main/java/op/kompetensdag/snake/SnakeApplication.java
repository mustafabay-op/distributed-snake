package op.kompetensdag.snake;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.HeadDirectionRecord;
import op.kompetensdag.snake.processors.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class SnakeApplication {

    public static void main(String[] args) {
        SpringApplication.run(SnakeApplication.class, args);

        // Configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        Map<String, String> schemaRegistryProps = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));


        // Serializing/Deserializing
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();

        gameStatusSerde.configure(schemaRegistryProps, false);
        headDirSerde.configure(schemaRegistryProps, false);
        gameTableEntrySerde.configure(schemaRegistryProps, false);

        // Build
        final StreamsBuilder builder = new StreamsBuilder();
        KTable<String, HeadDirectionRecord> currentHeadDirectionTable = builder.table(Topics.HEAD_DIRECTION, Consumed.with(Serdes.String(), headDirSerde));
        KStream<String, GameTableEntry> tableEntryLog = builder.stream(Topics.GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));
        KTable<String, GameStatusRecord> gameStatusKTable = builder.table(Topics.GAME_STATUS, Consumed.with(Serdes.String(), gameStatusSerde));

        // Define processors
        GameInputRouter.define(builder, schemaRegistryProps);
        AdministrationProcessor.define(builder, schemaRegistryProps, gameStatusKTable);
        MovementProcessor.define(builder, schemaRegistryProps, gameStatusKTable, currentHeadDirectionTable);
        TickGenerator.define(gameStatusKTable, schemaRegistryProps);
        TickProcessor.define(builder, schemaRegistryProps, currentHeadDirectionTable, tableEntryLog);
        GameUpdatesProcessor.define(tableEntryLog);


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}