package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.events.GameAdministrationKeyPressed;
import op.kompetensdag.snake.events.GameMovementKeyPressed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class GameInputRouter {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps){
        SpecificAvroSerde<GameMovementCommand> gameMovementCommandSpecificAvroSerde = new SpecificAvroSerde<>();
        gameMovementCommandSpecificAvroSerde.configure(schemaRegistryProps, false);

        BranchedKStream<String, String> gameInputBranched =
                builder.stream(GAME_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())).split();

        // return DirectionParser.parse(v.getCOMMAND()) != null;

        gameInputBranched.branch(
                (k, v) -> GameMovementCommandParser.parse(v) != null,
                Branched.withConsumer(stream -> stream.mapValues((k, v) -> GameMovementCommandParser.parse(v))
                        .to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))));

/*        gameInputBranched.branch(
                (k, v) -> GameAdministrationKeyPressed.isValidValue(v),
                Branched.withConsumer(stream -> stream.mapValues((k,v) -> GameAdministrationKeyPressed.valueOf(v))
                        .to(GAME_ADMINISTRATION_COMMANDS_TOPIC, Produced.with(Serdes.String(), CustomSerdes.gameAdministrationKeyPressedSerde))));*/

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(
                        v -> GameMovementCommandParser.parse("DOWN")
                ).to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))));

        builder.stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementCommandSpecificAvroSerde))
                .mapValues(v -> v.KEYPRESSED + " : " + v.getKEYPRESSED() + "processed " + v.getClass())
                .to(GAME_OUTPUT);


    }


}
