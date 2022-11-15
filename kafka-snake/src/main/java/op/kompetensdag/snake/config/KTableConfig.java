package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.HeadDirectionRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static op.kompetensdag.snake.Topics.GAME_STATUS_TOPIC;

@Configuration
public class KTableConfig {

    @Bean
    public KTable<String, HeadDirectionRecord> headDirectionRecordKTable3(final StreamsBuilder streamsBuilder,
                                                                          final SpecificAvroSerde<HeadDirectionRecord> headDirSerde) {
        return streamsBuilder.table(Topics.HEAD_DIRECTION_TOPIC_3, Consumed.with(Serdes.String(), headDirSerde));
    }

    @Bean
    KTable<String, GameStatusRecord> gameStatusKTable(final StreamsBuilder streamsBuilder,
                                                      final SpecificAvroSerde<GameStatusRecord> gameStatusSerde) {
        return streamsBuilder.table(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde));
    }

}
