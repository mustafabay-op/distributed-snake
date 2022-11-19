package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.HeadDirectionRecord;
import op.kompetensdag.snake.processors.MovementProcessor;
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
                // Here we can make use of the builder pattern to continuously decorate and process an event throughout
                // a KStream/KTable processing flow.
                // Inside the MovementProcessor class you will find the values which are needed to validate the movement event
                // Currently the MovementProcessor builder is decorated with the intended head direction.
                // 1. Join the stream with gameStatusKTable and add the current status of the game to the MovementProcessor builder
                //
                // 2. Use filter to assert that the game status you added equals GameStatus.RUNNING
                //
                // 3. Now join with headDirectionRecordKTable and add the current head direction to the builder.
                //
                // 4. Use filter to validate the intended head direction with the current head direction
                //
                // 5. The validation is done. Map the event to a HeadDirectionRecord
                //
                // Hint: Use the helper methods in processors/MovementProcessor

                // Add your code above this line.

                .to(Topics.HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));
        return null;
    }

    @Bean
    public KTable<String, HeadDirectionRecord> headDirectionRecordKTable(final StreamsBuilder streamsBuilder,
                                                                          final SpecificAvroSerde<HeadDirectionRecord> headDirSerde) {
        return streamsBuilder.table(Topics.HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde));
    }



}
