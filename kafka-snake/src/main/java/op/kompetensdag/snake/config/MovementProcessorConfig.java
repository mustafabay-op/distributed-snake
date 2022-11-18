package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.HeadDirectionRecord;
import op.kompetensdag.snake.processors.MovementProcessor;
import op.kompetensdag.snake.util.Join;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static op.kompetensdag.snake.Topics.GAME_MOVEMENT_COMMANDS_TOPIC;

@Configuration
public class MovementProcessorConfig {

    @Bean
    public Object movementProcessingTopology(final StreamsBuilder streamsBuilder,
                                             final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde,
                                             final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                                             final KTable<String, GameStatusRecord> gameStatusKTable,
                                             final KTable<String, HeadDirectionRecord> headDirectionRecordKTable3) {
        streamsBuilder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .join(gameStatusKTable, (movementCommand, gameStatus) -> new Join<>(movementCommand, gameStatus))
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getType().equals(GameStatus.RUNNING))
                .mapValues(movementAndStateJoin -> movementAndStateJoin.l())
                .join(headDirectionRecordKTable3, (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection))
                .mapValues(movementAndHeadDirectionJoin -> new Join<>(MovementProcessor.getIntendedHeadDirection(movementAndHeadDirectionJoin.l().getType()), movementAndHeadDirectionJoin.r().getType()))
                .filter((k, intendedAndCurrentHeadDirection) -> MovementProcessor.isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(join -> new HeadDirectionRecord(join.l()))
                .to(Topics.HEAD_DIRECTION_TOPIC_3, Produced.with(Serdes.String(), headDirSerde));
        return null;
    }

    @Bean
    public KTable<String, HeadDirectionRecord> headDirectionRecordKTable3(final StreamsBuilder streamsBuilder,
                                                                          final SpecificAvroSerde<HeadDirectionRecord> headDirSerde) {
        return streamsBuilder.table(Topics.HEAD_DIRECTION_TOPIC_3, Consumed.with(Serdes.String(), headDirSerde));
    }

}
