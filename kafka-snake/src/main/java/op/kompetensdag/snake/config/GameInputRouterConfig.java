package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameAdministrationCommand;
import op.kompetensdag.snake.model.GameAdministrationCommandRecord;
import op.kompetensdag.snake.model.GameMovementKeyPressed;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import op.kompetensdag.snake.processors.GameInputRouter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static op.kompetensdag.snake.config.Topics.*;

@Configuration
public class GameInputRouterConfig {

    @Bean
    public Object gameInputRoutingTopology(final StreamsBuilder streamsBuilder,
                                           // These serialization and deserialization beans are to be used
                                           // when producing/consuming to/from a topic
                                           final SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde,
                                           final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde) {
        return streamsBuilder
                .stream(GAME_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .split()
                // Use the .branch operation to add conditional branching.
                // 1. If the record value is a game movement event create a GameMovementKeyPressedRecord
                //    and send it to the GAME_MOVEMENT_COMMANDS_TOPIC.
                // 2. If the record value is a game administration event, create a GameAdministrationCommandRecord
                //    and send it to the GAME_ADMINISTRATION_COMMANDS_TOPIC
                //
                // Hint: Use the helper methods in processors/GameInputRouter

                // Add your code above this line.
                .defaultBranch(
                        Branched.withConsumer(stream -> stream.mapValues(v -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.LEFT))
                                .to(ILLEGAL_ARGUMENTS_TOPIC, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));
    }


}
