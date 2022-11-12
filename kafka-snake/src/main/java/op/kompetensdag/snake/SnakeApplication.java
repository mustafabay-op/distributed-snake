package op.kompetensdag.snake;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.processors.TickGenerator;
import op.kompetensdag.snake.processors.TickProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static op.kompetensdag.snake.Topics.GAME_STATUS_TOPIC;

@SpringBootApplication
public class SnakeApplication {


    public static void main(String[] args) {
        SpringApplication.run(SnakeApplication.class, args);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");

        Map<String, String> schemaRegistryProps = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));

        final StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);

        KTable<String, GameStatusRecord> gameStatusKTable = builder.table(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde), Materialized.with(Serdes.String(), gameStatusSerde));


        GameInputRouter.define(builder, schemaRegistryProps);
        AdministrationProcessor.define(builder, schemaRegistryProps, gameStatusKTable);
        MovementProcessor.define(builder, schemaRegistryProps, gameStatusKTable);
        TickProcessor.define(builder,schemaRegistryProps);
        TickGenerator.define(gameStatusKTable,schemaRegistryProps);

        //GameStatusUpdateRouter.define(builder, schemaRegistryProps);
        // InitializeGameProcessor.define(builder, schemaRegistryProps);

        // builder.stream(GAME_INITIALIZING_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde))


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