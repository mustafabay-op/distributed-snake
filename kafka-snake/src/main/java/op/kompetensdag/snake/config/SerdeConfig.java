package op.kompetensdag.snake.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Configuration
public class SerdeConfig {

    public static final String SCHEMA_REGISTRY_URL_PROP = "schema.registry.url";

    @Bean
    public Map<String, String> schemaRegistryProps(final Properties streamProperties) {
        return Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, streamProperties.getProperty(SCHEMA_REGISTRY_URL_PROP));
    }

    @Bean
    public SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);
        return gameMovementKeyPressedSerde;
    }

    @Bean
    public SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde = new SpecificAvroSerde<>();
        gameAdministrationSerde.configure(schemaRegistryProps, false);
        return gameAdministrationSerde;
    }

    @Bean
    public SpecificAvroSerde<GameStatusRecord> gameStatusSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        return gameStatusSerde;
    }

    @Bean
    public SpecificAvroSerde<HeadDirectionRecord> headDirSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);
        return headDirSerde;
    }

    @Bean
    public SpecificAvroSerde<GameTableEntry> gameTableEntrySerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();
        gameTableEntrySerde.configure(schemaRegistryProps, false);
        return gameTableEntrySerde;
    }

    @Bean
    public SpecificAvroSerde<GameTick> tickSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameTick> tickSerde = new SpecificAvroSerde<>();
        tickSerde.configure(schemaRegistryProps, false);
        return tickSerde;
    }

    @Bean
    public SpecificAvroSerde<GameTablePosition> gameTablePositionSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameTablePosition> gameTablePositionSerde = new SpecificAvroSerde<>();
        gameTablePositionSerde.configure(schemaRegistryProps, true);
        return gameTablePositionSerde;
    }

    @Bean
    public SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde = new SpecificAvroSerde<>();
        gameTableEntriesSerde.configure(schemaRegistryProps, false);
        return gameTableEntriesSerde;
    }

    @Bean
    public SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde(final Map<String, String> schemaRegistryProps) {
        SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde = new SpecificAvroSerde<>();
        processTickCommandSerde.configure(schemaRegistryProps, false);
        return processTickCommandSerde;
     }
}
