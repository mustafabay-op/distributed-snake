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

import static op.kompetensdag.snake.Topics.*;

@Configuration
public class GameInputRouterConfig {

    @Bean
    public Object gameInputRoutingTopology(final StreamsBuilder streamsBuilder,
                                           final SpecificAvroSerde<GameAdministrationCommandRecord> gameAdministrationSerde,
                                           final SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde) {
        return streamsBuilder
                .stream(GAME_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .split()
                .branch(GameInputRouter.isGameMovementKeyPressedEvent(),
                        Branched.withConsumer(stream -> stream.mapValues(value -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.valueOf(value)))
                                .to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))))
                .branch(GameInputRouter.isGameAdministrationKeyPressedEvent(),
                        Branched.withConsumer(stream -> stream.mapValues(value -> new GameAdministrationCommandRecord(GameAdministrationCommand.valueOf(value)))
                                .to(GAME_ADMINISTRATION_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameAdministrationSerde))))
                .defaultBranch(
                        Branched.withConsumer(stream -> stream.mapValues(v -> new GameMovementKeyPressedRecord(GameMovementKeyPressed.LEFT))
                                .to(ILLEGAL_ARGUMENTS_TOPIC, Produced.with(Serdes.String(), gameMovementKeyPressedSerde))));
    }


}
