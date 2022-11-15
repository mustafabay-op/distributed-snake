package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameTableEntry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static op.kompetensdag.snake.Topics.GAME_TABLE_ENTRIES;

@Configuration
public class KStreamConfig {

    @Bean
    public KStream<String, GameTableEntry> tableEntryLog(final StreamsBuilder streamsBuilder,
                                                         final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return streamsBuilder.stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));
    }
}
