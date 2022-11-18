package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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

import static op.kompetensdag.snake.config.Topics.GAME_MOVEMENT_COMMANDS_TOPIC;

@Configuration
public class MovementProcessorConfig {

    @Bean
    public Object movementProcessingTopology(final StreamsBuilder streamsBuilder,
                                             final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde,
                                             final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                                             final KTable<String, GameStatusRecord> gameStatusKTable,
                                             final KTable<String, HeadDirectionRecord> headDirectionRecordKTable) {
        streamsBuilder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .mapValues((gameId, movement) -> MovementProcessor.builder().intendedHeadDirection(MovementProcessor.getIntendedHeadDirection(movement.getType())))
                .join(gameStatusKTable, MovementProcessor.MovementProcessorBuilder::gameStatus)
                .filter((gameId, builder) -> builder.build().getGameStatus().equals(new GameStatusRecord(GameStatus.RUNNING)))
                .join(headDirectionRecordKTable, MovementProcessor.MovementProcessorBuilder::currentHeadDirection)
                .filter((gameId, builder) -> MovementProcessor.isIntendedMoveValid(builder.build().getCurrentHeadDirection(), builder.build().getIntendedHeadDirection()))
                .mapValues(builder -> new HeadDirectionRecord(builder.build().getIntendedHeadDirection().getType()))
                .to(Topics.HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));
        return null;
    }

    @Bean
    public KTable<String, HeadDirectionRecord> headDirectionRecordKTable(final StreamsBuilder streamsBuilder,
                                                                          final SpecificAvroSerde<HeadDirectionRecord> headDirSerde) {
        return streamsBuilder.table(Topics.HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde));
    }



}
